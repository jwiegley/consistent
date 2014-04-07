{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE TypeFamilies #-}

module Control.Concurrent.Consistent
    ( ConsistentT
    , CTMT
    , runConsistentT
    , consistently
    , CVar
    , newCVar
    , dupCVar
    , readCVar
    , writeCVar
    , swapCVar
    , modifyCVar
    ) where

import Control.Applicative
import Control.Concurrent
import Control.Concurrent.Async.Lifted
import Control.Concurrent.STM
import Control.Exception.Lifted (bracket_)
import Control.Monad
import Control.Monad.Base
import Control.Monad.IO.Class
import Control.Monad.Trans.Control
import Control.Monad.Trans.Reader
import Control.Monad.Trans.Writer
import Data.HashMap.Strict as Map
import Data.Maybe
import Prelude

data ConsistentState = ConsistentState
    { csActiveThreads :: TVar Int
    , csJournal       :: TQueue [STM (Maybe (STM ()))]
    }

newtype ConsistentT m a = ConsistentT
    { getConsistentT :: ReaderT ConsistentState m a }
    deriving (Functor, Applicative, Monad, MonadIO)

instance MonadBase IO m => MonadBase IO (ConsistentT m) where
    liftBase b = ConsistentT $ liftBase b

instance MonadBaseControl IO m => MonadBaseControl IO (ConsistentT m) where
    newtype StM (ConsistentT m) a =
        StMConsistentT (StM (ReaderT ConsistentState m) a)
    liftBaseWith f =
        ConsistentT $ liftBaseWith $ \runInBase -> f $ \k ->
            liftM StMConsistentT $ runInBase $ getConsistentT k
    restoreM (StMConsistentT m) = ConsistentT . restoreM $ m

newtype CTMT m a = CTMT (WriterT [STM (Maybe (STM ()))] (ConsistentT m) a)
    deriving (Functor, Applicative, Monad, MonadIO)

runConsistentT :: (MonadBaseControl IO m, MonadIO m) => ConsistentT m a -> m a
runConsistentT action = do
    cs <- liftIO $ ConsistentState <$> newTVarIO 0 <*> newTQueueIO
    flip runReaderT cs
        $ withAsync (liftIO (applyChanges cs))
        $ \worker -> do
            link worker
            getConsistentT action
  where
    applyChanges ConsistentState {..} = forever $ atomically $ do
        -- Process only if no threads are in a "consistently" block.
        active <- readTVar csActiveThreads
        check (active == 0)

        -- Read the next set of updates to apply.
        updates <- sequence =<< readTQueue csJournal

        -- If any component of the update fails the generational check (see
        -- 'postUpdate'), drop the update for consistency's sake.
        when (all isJust updates) $ sequence_ (catMaybes updates)

consistently :: (MonadBaseControl IO m, MonadIO m)
           => CTMT m a -> ConsistentT m a
consistently (CTMT f) = do
    ConsistentState {..} <- ConsistentT ask
    let active = liftIO . atomically . modifyTVar csActiveThreads
    bracket_ (active succ) (active pred) $ do
        (a, updates) <- runWriterT f
        liftIO $ atomically $ writeTQueue csJournal updates
        return a

data ConsistentData a = ConsistentData
    { cdVars    :: TVar (HashMap ThreadId (a, TVar Int))
      -- ^ The @TVar Int@ in cdVars is the cdCurrGen from each
      -- 'ConsistentData' value.
    , cdMyData  :: a
    , cdBaseGen :: TVar Int
    , cdCurrGen :: TVar Int
    }

newtype CVar a = CVar { getCVar :: ConsistentData (TVar a) }

newCData :: MonadIO m => a -> (a -> IO v) -> m (ConsistentData v)
newCData a f = liftIO $ do
    me   <- f a
    bgen <- newTVarIO 0
    cgen <- newTVarIO 0
    tid  <- myThreadId
    vars <- newTVarIO (Map.singleton tid (me, cgen))
    return $ ConsistentData vars me bgen cgen

dupCData :: MonadIO m => ConsistentData v -> (v -> STM v) -> m (ConsistentData v)
dupCData (ConsistentData vs v _ _) f = liftIO $ do
    tid <- myThreadId
    atomically $ do
        me   <- f v
        bgen <- newTVar 0
        cgen <- newTVar 0
        modifyTVar' vs (Map.insert tid (me, cgen))
        return $ ConsistentData vs me bgen cgen

newCVar :: MonadIO m => a -> m (CVar a)
newCVar a = CVar `liftM` newCData a newTVarIO

dupCVar :: MonadIO m => CVar a -> m (CVar a)
dupCVar (CVar cd) = CVar `liftM` dupCData cd (newTVar <=< readTVar)

readCVar :: MonadIO m => CVar a -> m a
readCVar = liftIO . readTVarIO . cdMyData . getCVar

writeCVar :: MonadIO m => CVar a -> a -> CTMT m ()
writeCVar  (CVar cd) a = do
    liftIO $ atomically $ writeTVar (cdMyData cd) a
    CTMT $ tell [updateData cd writeTVar a]

updateData :: ConsistentData v -> (v -> a -> STM ()) -> a -> STM (Maybe (STM ()))
updateData (ConsistentData vs _ bgen cgen) write a = do
    -- Get the base generation for this CVar, or what we last knew it to
    -- be, and the current generation, which can be updated by other
    -- threads if their updates succeed.
    base <- readTVar bgen
    curr <- readTVar cgen

    -- If base is not the same as curr, another thread has changed the CVar,
    -- and so any changes in the current consistent block are now invalid.
    -- Return Nothing so that 'applyChanges' throws them away.
    return $
        if base /= curr
        then Nothing
        else Just $ do
            -- Otherwise, move on to the next generation...
            let next = succ base
            writeTVar bgen next
            writeTVar cgen next

            -- Update every thread with the new value and generation.
            -- This causes pending updates in other threads involving the
            -- same variable to become invalid.
            vars <- Map.elems <$> readTVar vs
            forM_ vars $ \(o, ogen) -> do
                write o a
                writeTVar ogen next

            -- jww (2014-04-07): Performance issue: Multiple calls to
            -- writeCVar from the same block will cause this loop to
            -- execute that many times.

swapCVar :: MonadIO m => CVar a -> a -> CTMT m a
swapCVar v y = do
    x <- readCVar v
    writeCVar v y
    return x

modifyCVar :: MonadIO m => CVar a -> (a -> a) -> CTMT m ()
modifyCVar v f = writeCVar v . f =<< readCVar v

-- newtype CQueue a = CQueue { getCQueue :: ConsistentData (TQueue a) }

-- newCQueue :: MonadIO m => m (CQueue a)
-- newCQueue = CQueue `liftM` newCData (error "unused") (const newTQueueIO)

-- dupCQueue :: MonadIO m => CQueue a -> m (CQueue a)
-- dupCQueue (CQueue cd) = CQueue `liftM` dupCData cd (newTVar <=< readTVar)

-- readCQueue :: MonadIO m => CQueue a -> m a
-- readCQueue = liftIO . atomically . readTQueue . cdMyData . getCQueue

-- writeCQueue :: MonadIO m => CQueue a -> a -> CTMT m ()
-- writeCQueue  (CQueue cd) a = do
--     liftIO $ atomically $ writeTQueue (cdMyData cd) a
--     CTMT $ tell [updateData cd writeTQueue a]
