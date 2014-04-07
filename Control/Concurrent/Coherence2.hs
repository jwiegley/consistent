{-# LANGUAGE ExistentialQuantification #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE TypeFamilies #-}

module Control.Concurrent.Coherence where

import Control.Applicative
import Control.Concurrent
import Control.Concurrent.Async.Lifted
import Control.Concurrent.STM
import Control.Exception.Lifted (bracket_)
import Control.Monad hiding (forM_, mapM_)
import Control.Monad.Base
import Control.Monad.IO.Class
import Control.Monad.Reader.Class
import Control.Monad.Trans.Control
import Control.Monad.Trans.Reader (ReaderT(..), runReaderT)
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
    cs <- liftIO $ do
        m  <- newTQueueIO
        v  <- newTVarIO False
        return $ CoherenceState m v

    flip runReaderT cs $ withAsync applyChanges $ \worker -> do
        link worker
        runCoherenceT action
  where
    applyChanges = do
        CoherenceState {..} <- ask
        liftIO $ forever $ atomically $ do
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
        a  <- readTVar v
        me <- newTVar a
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
                    error "Modified the same CVar in multiple coherent blocks"
                _ -> do
                    writeTVar me a
                    writeTVar vmod (Just tid)
                    writeTQueue csJournal (apply tid)
  where
    apply tid = do
        val <- readTVar me
        foldlWithKey'
            (\f other o ->
              f >> unless (other == tid) (writeTVar o val))
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

main :: IO ()
main = runCoherently $ do
    v <- newCVar (10 :: Int)
    u <- newCVar (20 :: Int)
    withAsync (worker v u) $ \thread -> do
        replicateM_ 30 $ do
            liftIO $ print "parent before"
            coherently $ do
                x <- readCVar v
                writeCVar u 100
                writeCVar v 300
                liftIO $ print $ "parent: " ++ show x
            liftIO $ print "parent end"
            liftIO $ threadDelay 50000
        wait thread
    liftIO $ print "exiting!"
  where
    worker pv pu = do
        v <- dupCVar pv
        u <- dupCVar pu
        liftIO $ threadDelay 100000
        replicateM_ 30 $ do
            liftIO $ print "child before"
            coherently $ do
                x <- readCVar u
                writeCVar v 200
                liftIO $ print $ "child: " ++ show x
            liftIO $ print "child before"
            liftIO $ threadDelay 450000
