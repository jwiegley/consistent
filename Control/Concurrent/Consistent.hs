{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE TypeFamilies #-}

module Control.Concurrent.Consistent where

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

data ConsistentState = ConsistentState
    { csJournal       :: TQueue (STM ())
    , csActiveThreads :: TVar Int
    }

newtype ConsistentT m a = ConsistentT
    { runConsistentT :: ReaderT ConsistentState m a }
    deriving (Functor, Applicative, Monad, MonadIO)

instance MonadBase IO m => MonadBase IO (ConsistentT m) where
    liftBase b = ConsistentT $ liftBase b

instance MonadBaseControl IO m => MonadBaseControl IO (ConsistentT m) where
    newtype StM (ConsistentT m) a =
        StMConsistentT (StM (ReaderT ConsistentState m) a)
    liftBaseWith f =
        ConsistentT $ liftBaseWith $ \runInBase -> f $ \k ->
            liftM StMConsistentT $ runInBase $ runConsistentT k
    restoreM (StMConsistentT m) = ConsistentT . restoreM $ m

runConsistently :: (MonadBaseControl IO m, MonadIO m) => ConsistentT m a -> m a
runConsistently action = do
    cs <- liftIO $ ConsistentState <$> newTQueueIO <*> newTVarIO 0
    flip runReaderT cs $ withAsync (liftIO (applyChanges cs)) $ \worker ->
        link worker >> runConsistentT action
  where
    applyChanges ConsistentState {..} = forever $ atomically $ do
        active <- readTVar csActiveThreads
        check (active == 0)
        mt <- isEmptyTQueue csJournal
        check (not mt)

        -- Only process 10 actions from the queue at a time, to make sure
        -- we don't try to do too much and end up getting starved.
        replicateM_ 10 $ do
            mt' <- isEmptyTQueue csJournal
            unless mt' $ join $ readTQueue csJournal

newCVar :: MonadIO m => a -> ConsistentT m (CVar a)
newCVar a = ConsistentT $ liftIO $ do
    tid <- myThreadId
    atomically $ do
        me <- newTVar a
        CVar <$> newTVar (Map.singleton tid me)
             <*> pure me
             <*> newTVar Nothing

dupCVar :: MonadIO m => CVar a -> ConsistentT m (CVar a)
dupCVar (CVar vs v vmod) = ConsistentT $ liftIO $ do
    tid <- myThreadId
    atomically $ do
        me <- newTVar =<< readTVar v
        modifyTVar vs (Map.insert tid me)
        return $ CVar vs me vmod

readCVar :: MonadIO m => CVar a -> ConsistentT m a
readCVar (CVar _ me _) = ConsistentT $ liftIO $ readTVarIO me

writeCVar :: MonadIO m => CVar a -> a -> ConsistentT m ()
writeCVar (CVar vs me vmod) a = ConsistentT $ do
    ConsistentState {..} <- ask
    liftIO $ do
        tid <- myThreadId
        atomically $ do
            m <- readTVar vmod
            case m of
                Just other | other /= tid ->
                    error "Modified CVar in multiple consistent blocks"
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

consistently :: (MonadBaseControl IO m, MonadIO m)
           => ConsistentT m a -> ConsistentT m a
consistently (ConsistentT f) = ConsistentT $ do
    ConsistentState {..} <- ask
    bracket_
        (liftIO $ atomically $ modifyTVar csActiveThreads succ)
        (liftIO $ atomically $ modifyTVar csActiveThreads pred) f
