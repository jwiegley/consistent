module Main where

import Control.Concurrent
import Control.Concurrent.Async.Lifted
import Control.Concurrent.Consistent
-- import Control.Exception
import Control.Monad hiding (forM_, mapM_)
import Control.Monad.IO.Class
import Debug.Trace
import Prelude hiding (log)
import Prelude hiding (log, mapM_)
--import Test.Hspec

-- tryAny :: IO a -> IO (Either SomeException a)
-- tryAny = try

-- main :: IO ()
-- main = hspec $ do
--     describe "simple logging" $ do
--         it "logs output" $ True `shouldBe` True

main :: IO ()
main = do
    test <- async $ void $ runConsistently $ do
        u <- newCVar 0
        v <- newCVar 0
        mapConcurrently worker $
            flip map [1..100 :: Int] $ \i -> (u, v, i)
    wait test
  where
    worker (pu, pv, i) = do
        u <- dupCVar pu
        v <- dupCVar pv
        replicateM_ 100 $ do
            consistently $ do
                x <- readCVar u
                writeCVar u i
                y <- readCVar v
                writeCVar v i
                trace (show (x, y)) $ return ()
            liftIO $ threadDelay (i * 1000)
