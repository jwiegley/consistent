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
import Control.Monad
import Control.Monad.Base
import Control.Monad.IO.Class
import Control.Monad.Trans.Control
import Control.Monad.Trans.Reader
import Control.Monad.Trans.Writer
import Data.HashMap.Strict as Map
import Data.Maybe
import Prelude

data CVar a = CVar
    { cvVars    :: TVar (HashMap ThreadId (TVar a, TVar Int))
      -- ^ The @TVar Int@ in cvVars is the cvCurrGen from each CVar.
    , cvMyVar   :: TVar a
    , cvBaseGen :: TVar Int
    , cvCurrGen :: TVar Int
    , cvOwner   :: ThreadId
    }

data ConsistentState = ConsistentState
    { csActiveThreads :: TVar Int
    , csJournal       :: TQueue [STM (Maybe (STM ()))]
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

newtype CTMT m a = CTMT
    { runCTMT :: WriterT [STM (Maybe (STM ()))] (ConsistentT m) a}
    deriving (Functor, Applicative, Monad, MonadIO)

runConsistently :: (MonadBaseControl IO m, MonadIO m) => ConsistentT m a -> m a
runConsistently action = do
    cs <- liftIO $ ConsistentState <$> newTVarIO 0 <*> newTQueueIO
    flip runReaderT cs
        $ withAsync (liftIO (applyChanges cs))
        $ \worker -> do
            link worker
            runConsistentT action

newCVar :: MonadIO m => a -> m (CVar a)
newCVar a = liftIO $ do
    me   <- newTVarIO a
    bgen <- newTVarIO 0
    cgen <- newTVarIO 0
    tid  <- myThreadId
    vars <- newTVarIO (Map.singleton tid (me, cgen))
    return $ CVar vars me bgen cgen tid

dupCVar :: MonadIO m => CVar a -> m (CVar a)
dupCVar cv@(CVar vs v _ _ owner) = liftIO $ do
    tid <- myThreadId
    if tid == owner
        then return cv
        else atomically $ do
            me   <- newTVar =<< readTVar v
            bgen <- newTVar 0
            cgen <- newTVar 0
            modifyTVar' vs (Map.insert tid (me, cgen))
            return $ CVar vs me bgen cgen tid

readCVar :: MonadIO m => CVar a -> m a
readCVar (CVar _ me _ _ _) = liftIO $ readTVarIO me

writeCVar :: (MonadIO m, Show a) => CVar a -> a -> CTMT m ()
writeCVar cv@(CVar _ me _ _ _) a = do
    liftIO $ atomically $ writeTVar me a
    CTMT $ tell [postUpdate cv a]

consistently :: (MonadBaseControl IO m, MonadIO m)
           => CTMT m a -> ConsistentT m a
consistently (CTMT f) = do
    ConsistentState {..} <- ConsistentT ask
    let active = liftIO . atomically . modifyTVar csActiveThreads
    bracket_ (active succ) (active pred) $ do
        (a, updates) <- runWriterT f
        liftIO $ atomically $ writeTQueue csJournal updates
        return a

postUpdate :: Show a => CVar a -> a -> STM (Maybe (STM ()))
postUpdate (CVar vs _ bgen cgen _) val = do
    -- Get the base generation for this CVar, or what we last knew it to be,
    -- and the current generation, which can be updated by other threads if
    -- their updates succeed.
    base <- readTVar bgen
    curr <- readTVar cgen

    -- If base is not the same as curr, another thread has changed the CVar, and
    -- so any changes in the current consistent block are now invalid.  Return
    -- Nothing so that 'applyChanges' throws them away.
    return $
        if base /= curr
        then Nothing
        else Just $ do
            -- Otherwise, move on to the next generation...
            let next = succ base
            writeTVar bgen next
            writeTVar cgen next

            -- Update every thread with the new value and generation.  This
            -- causes pending updates in other threads involving the same
            -- variable to become invalid.
            -- jww (2014-04-07): Performance issue: Multiple calls to
            -- writeCVar from the same block will cause this loop to execute
            -- that many times.
            vars <- Map.elems <$> readTVar vs
            forM_ vars $ \(o, ogen) -> do
                writeTVar o val
                writeTVar ogen next

applyChanges :: ConsistentState -> IO b
applyChanges ConsistentState {..} = forever $ atomically $ do
    -- Process updates only when no threads are in a "consistently" block.
    active <- readTVar csActiveThreads
    check (active == 0)

    -- Read the next set of updates to apply.
    updates <- sequence =<< readTQueue csJournal

    -- If any component of the update fails the generational check (see
    -- 'postUpdate'), drop the update for consistency's sake.
    when (all isJust updates) $ sequence_ (catMaybes updates)
