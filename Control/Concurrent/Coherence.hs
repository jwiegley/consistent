{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}

module Control.Concurrent.Coherence where

import Control.Applicative
import Control.Concurrent (threadDelay)
import Control.Concurrent.Async.Lifted
import Control.Concurrent.STM
import Control.Exception.Lifted (bracket_)
import Control.Monad hiding (forM_, mapM_)
import Control.Monad.Base
import Control.Monad.IO.Class
import Control.Monad.Loops
import Control.Monad.Reader.Class
import Control.Monad.Trans.Control
import Control.Monad.Trans.Reader (ReaderT(..), runReaderT)
import Prelude hiding (log, mapM_)

data ModificationKind
    = NeitherModified
    | ParentModified
    | ChildModified
    | BothModified
    deriving (Show, Eq)

data CVar a = CVar
    { cvLeft     :: TVar a
    , cvRight    :: TVar a
    , cvModified :: TVar ModificationKind
    }

data WhichSide = ParentThread | ChildThread deriving (Show, Eq)

data CoherenceState = CoherenceState
    { csSide    :: WhichSide
    , csJournal :: TQueue (STM ())
    , csSyncVar :: TVar Bool
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
        return $ CoherenceState ParentThread m v

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
newCVar a = CoherenceT $ liftIO $ atomically $
    CVar <$> newTVar a
         <*> newTVar a
         <*> newTVar NeitherModified

readCVar :: MonadIO m => CVar a -> CoherenceT m a
readCVar (CVar lv rv _) = CoherenceT $ do
    CoherenceState {..} <- ask
    liftIO $ readTVarIO $ if csSide == ParentThread then lv else rv

writeCVar :: MonadIO m => CVar a -> a -> CoherenceT m ()
writeCVar (CVar lv rv modKind) a = CoherenceT $ do
    CoherenceState {..} <- ask
    liftIO $ atomically $ do
        mk <- readTVar modKind
        let f x = if mk == NeitherModified then x else BothModified
        if csSide == ParentThread
            then do
                writeTVar lv a
                writeTVar modKind (f ParentModified)
            else do
                writeTVar rv a
                writeTVar modKind (f ChildModified)
        writeTQueue csJournal apply
  where
    apply = do
        mk <- readTVar modKind
        case mk of
            NeitherModified -> return ()
            BothModified    -> error "Incoherent state"
            ParentModified  -> readTVar lv >>= writeTVar rv
            ChildModified   -> readTVar rv >>= writeTVar lv
        writeTVar modKind NeitherModified

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
        replicateM_ 5 $ do
            liftIO $ print "parent before"
            coherently $ do
                x <- readCVar v
                writeCVar u 100
                writeCVar v 300
                liftIO $ print $ "parent: " ++ show x
            liftIO $ print "parent end"
            liftIO $ threadDelay 50000
        wait thread
    return ()
  where
    worker v u = do
        liftIO $ threadDelay 100000
        replicateM_ 5 $ do
            liftIO $ print "child before"
            coherently $ do
                x <- readCVar u
                writeCVar v 200
                liftIO $ print $ "child: " ++ show x
            liftIO $ print "child before"
            liftIO $ threadDelay 300000
