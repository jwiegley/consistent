{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE TypeFamilies #-}

module Control.Concurrent.Coherent where

import Control.Applicative
import Control.Concurrent
import Control.Concurrent.Async.Lifted
import Control.Concurrent.STM
import Control.Exception.Lifted (bracket_)
import Control.Monad hiding (forM_, mapM_)
import Control.Monad.Base
import Control.Monad.IO.Class
import Control.Monad.Trans.Control
import Control.Monad.Trans.Reader
import Data.HashMap.Strict as Map
import Prelude hiding (log, mapM_)

data CVar a = CVar
    { cvVars     :: TVar (HashMap ThreadId (TVar a))
    , cvMyVar    :: TVar a
    , cvModified :: TVar (Maybe ThreadId)
    }

data CoherenceState = CoherenceState
    { csJournal    :: TQueue (STM ())
    , csSyncVar    :: TVar Bool
    }

newtype CoherenceT m a = CoherenceT
    { runCoherenceT :: ReaderT CoherenceState m a }
    deriving (Functor, Applicative, Monad, MonadIO)

instance MonadBase IO m => MonadBase IO (CoherenceT m) where
    liftBase b = CoherenceT $ liftBase b

instance MonadBaseControl IO m => MonadBaseControl IO (CoherenceT m) where
    newtype StM (CoherenceT m) a =
        StMCoherenceT (StM (ReaderT CoherenceState m) a)
    liftBaseWith f =
        CoherenceT $ liftBaseWith $ \runInBase -> f $ \k ->
            liftM StMCoherenceT $ runInBase $ runCoherenceT k
    restoreM (StMCoherenceT m) = CoherenceT . restoreM $ m

runCoherently :: (MonadBaseControl IO m, MonadIO m) => CoherenceT m a -> m a
runCoherently action = do
    cs <- liftIO $ CoherenceState <$> newTQueueIO <*> newTVarIO False
    flip runReaderT cs $ withAsync (liftIO (applyChanges cs)) $ \worker ->
        link worker >> runCoherenceT action
  where
    applyChanges CoherenceState {..} = forever $ atomically $ do
        sv <- readTVar csSyncVar
        check (not sv)
        mt <- isEmptyTQueue csJournal
        check (not mt)

        -- Only process 10 actions from the queue at a time, to make sure
        -- we don't try to do too much and end up getting starved.
        replicateM_ 10 $ do
            mt' <- isEmptyTQueue csJournal
            unless mt' $ join $ readTQueue csJournal

newCVar :: MonadIO m => a -> CoherenceT m (CVar a)
newCVar a = CoherenceT $ liftIO $ do
    tid <- myThreadId
    atomically $ do
        me <- newTVar a
        CVar <$> newTVar (Map.singleton tid me)
             <*> pure me
             <*> newTVar Nothing

dupCVar :: MonadIO m => CVar a -> CoherenceT m (CVar a)
dupCVar (CVar vs v vmod) = CoherenceT $ liftIO $ do
    tid <- myThreadId
    atomically $ do
        me <- newTVar =<< readTVar v
        modifyTVar vs (Map.insert tid me)
        return $ CVar vs me vmod

readCVar :: MonadIO m => CVar a -> CoherenceT m a
readCVar (CVar _ me _) = CoherenceT $ liftIO $ readTVarIO me

writeCVar :: MonadIO m => CVar a -> a -> CoherenceT m ()
writeCVar (CVar vs me vmod) a = CoherenceT $ do
    CoherenceState {..} <- ask
    liftIO $ do
        tid <- myThreadId
        atomically $ do
            m <- readTVar vmod
            case m of
                Just other | other /= tid ->
                    error "Modified CVar in multiple coherent blocks"
                _ -> update tid csJournal
  where
    update tid csJournal = do
        writeTVar me a
        writeTVar vmod (Just tid)
        writeTQueue csJournal $ do
            val <- readTVar me
            foldlWithKey'
                (\f other o -> if other == tid
                               then f
                               else f >> writeTVar o val)
                (return ())
                =<< readTVar vs
            writeTVar vmod Nothing

coherently :: (MonadBaseControl IO m, MonadIO m)
           => CoherenceT m a -> CoherenceT m a
coherently (CoherenceT f) = CoherenceT $ do
    CoherenceState {..} <- ask
    bracket_
        (liftIO $ atomically $ writeTVar csSyncVar True)
        (liftIO $ atomically $ writeTVar csSyncVar False) f
