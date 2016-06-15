{-# LANGUAGE Rank2Types #-}

module Main where

import qualified Data.ByteString.Lazy as BL
import qualified Data.Text as T
import qualified Data.Text.Lazy.Encoding as TL
import Pipes
import qualified Pipes.Prelude as P
import Pipes.Safe
import qualified Pipes.Text as PT
import System.Directory

import Test.Tasty
import Test.Tasty.Golden

import Database.Kafkalite
import Database.Kafkalite.Backend.FileSystem
import Database.Kafkalite.Backend.Tar

main :: IO ()
main = defaultMain =<< testGroup "kafkalite" <$> sequence
    [ testBackend "filesystem backend" runFileSystemM'
    , testBackend "tar backend" runTarM'
    ]

testDataDir :: FilePath
testDataDir = "tests/test-data"

runFileSystemM' :: FileSystemM a -> IO a
runFileSystemM' x = runFileSystemM x (testDataDir ++ "/fs-data")

runTarM' :: TarM a -> IO a
runTarM' x = runTarM x (testDataDir ++ "/tar-data.tar")

-------------------------------------------------------------------------------
-- Testing the filesystem backend

testBackend :: (MonadKafka m, MonadThrow m) => String -> (forall a. m a -> IO a) -> IO TestTree
testBackend backendName runMonadKafka =
    testGroup backendName . map (testTopic runMonadKafka) <$>
        listDirectory (testDataDir ++ "/golden/")

testTopic :: (MonadKafka m, MonadThrow m) => (forall a. m a -> IO a) -> String -> TestTree
testTopic runMonadKafka testName =
    let (topicName, partition) = parseTopicPartition testName
    in goldenVsString
        ("topic " ++ testName)
        (testDataDir ++ "/golden/" ++ testName)
        (runMonadKafka $ exhaustTopic topicName partition)

parseTopicPartition :: String -> (String, Int)
parseTopicPartition xs = let (ls, rs) = span (/= '-') (reverse xs)
    in (reverse (drop 1 rs), read (reverse ls))

exhaustTopic :: (MonadKafka m, MonadThrow m) => String -> Int -> m BL.ByteString
exhaustTopic topicName partition = do
    topic <- loadTopic topicName partition
    fmap TL.encodeUtf8 $ PT.toLazyM $
        readTopic topic (Offset 0) >-> P.map (T.pack . (++ "\n") . ppMessage)
