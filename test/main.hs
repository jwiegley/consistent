module Main where

import Control.Concurrent
import Control.Concurrent.Async.Lifted
import Control.Concurrent.Consistent
import Control.Exception
import Control.Monad hiding (forM_, mapM_)
import Control.Monad.IO.Class
import Prelude hiding (log)
import Prelude hiding (log, mapM_)
--import Test.Hspec

tryAny :: IO a -> IO (Either SomeException a)
tryAny = try

-- main :: IO ()
-- main = hspec $ do
--     describe "simple logging" $ do
--         it "logs output" $ True `shouldBe` True

main :: IO ()
main = runConsistently $ do
    v <- newCVar (10 :: Int)
    u <- newCVar (20 :: Int)
    withAsync (worker v u) $ \thread -> do
        replicateM_ 30 $ do
            liftIO $ print "parent before"
            consistently $ do
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
            consistently $ do
                x <- readCVar u
                writeCVar v 200
                liftIO $ print $ "child: " ++ show x
            liftIO $ print "child before"
            liftIO $ threadDelay 400000
