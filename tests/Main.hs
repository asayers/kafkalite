
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

main :: IO ()
main = defaultMain =<< testFileSystemBackend

testDataDir = "tests/test-data"

-------------------------------------------------------------------------------
-- Testing the filesystem backend

testFileSystemBackend :: IO TestTree
testFileSystemBackend =
    testGroup "filesystem backend" . map testTopic <$>
        listDirectory (testDataDir ++ "/fs-golden/")

testTopic :: String -> TestTree
testTopic testName =
    let (topicName, partition) = parseTopicPartition testName
    in goldenVsString
        ("topic " ++ testName)
        (testDataDir ++ "/fs-golden/" ++ testName)
        (exhaustTopic topicName partition)

parseTopicPartition :: String -> (String, Int)
parseTopicPartition xs = let (ls, rs) = span (/= '-') (reverse xs)
    in (reverse (drop 1 rs), read (reverse ls))

exhaustTopic :: String -> Int -> IO BL.ByteString
exhaustTopic topicName partition = do
    let baseDir = testDataDir ++ "/fs-test"
    topic <- loadTopic baseDir topicName partition
    fmap TL.encodeUtf8 $ runSafeT $ PT.toLazyM $
        readTopic topic (Offset 0) >-> P.map (T.pack . (++ "\n") . ppMessage)
