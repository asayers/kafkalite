{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE Rank2Types #-}

-- | Read-only support for the Kafka on-disk format, no broker required!
--
-- This module provides access to Kafka streams which are accessible
-- directly on the local disk, without requiring a Kafka server. It
-- contains a parser for the Kafka on-disk log format, some logic for
-- seeking using Kafka's index files, and some logic for extracting
-- compressed streams. The idea is to provide an interface which is similar
-- to the one exposed by a real Kafka server, so that it shouldn't be too
-- tricky to transparently switch between online and offline access to
-- your Kafka data.
--
-- * Usage
--
-- > import Pipes
-- > import Pipes.Safe
-- > import Pipes.Prelude as P
-- >
-- > -- Read in and parse all index files for "myTopic"
-- > topic <- loadTopic
-- >    "/var/lib/kafka"  -- Kafka data directory
-- >    "myTopic"         -- Topic name
-- >    0                 -- Partition
-- >
-- > -- Stream messages from the topic, starting at the beginning (offset 0),
-- > -- and pretty-print them to stdout
-- > runSafeT $ runEffect $
-- >    readTopic topic (Offset 0) >-> P.map ppMessage >-> P.stdoutLn
--
-- * Operational properties
--
-- Memory use is constant-ish, but depends on:
--
-- - (In the case of uncompressed streams) the size of the messages;
-- - (In the case of compressed streams) the uncompressed size of message
--   batches;
-- - Also, indices are loaded into memory, which are variably sized and
--   various in number.
--
-- Files are opened only when needed and are closed promptly. When
-- streaming across a segment boundary, for instance, the first segment's
-- log file is closed as the second segment's log file is opened.
--
module Database.Kafkar
    ( loadTopic   -- :: FilePath -> String -> Int -> IO Topic
    , readTopic   -- :: Topic -> Offset -> Producer MessageEntry IO ()
    , ppMessage   -- :: MessageEntry -> String
    ) where

import Data.Traversable
import Control.Exception (throwIO)
import Control.Monad
import Control.Monad.IO.Class
import Data.AffineSpace
import qualified Data.Attoparsec.ByteString as AP
import Data.ByteString (ByteString)
import qualified Data.ByteString as BS
import qualified Data.List as L
import Data.Monoid
import qualified Data.Vector as V
import Pipes (Producer, (>->))
import Pipes.Safe (MonadSafe)
import qualified Pipes.Prelude as P
import qualified Pipes.Safe.Prelude as P
import qualified Pipes.Attoparsec as PAP
import qualified Pipes.ByteString as PBS
import System.Directory
import System.Exit
import System.FilePath
import qualified System.IO as IO

import Database.Kafkar.Compression
import Database.Kafkar.Parsers
import Database.Kafkar.Types
import Database.Kafkar.Util


-- | Just for testing in GHCi
loadTopic_ :: String -> IO Topic
loadTopic_ topicName =
    loadTopic "/var/lib/kafka" topicName 0

-- | This function takes the path of the Kafka log directory and the name
-- and partition of a topic contained within it, and returns a handle to
-- the topic's contents. It loads all the index files into memory (~10KB
-- per segment), but doesn't read any of the log data.
loadTopic :: FilePath -> String -> Int -> IO Topic
loadTopic logDir topicName partition = do
    let topicDir = logDir </> topicName ++ "-" ++ show partition
    listing <- map (topicDir </>) <$> listDirectory topicDir
    let logs    = L.sort $ filter (".log"   `L.isSuffixOf`) listing
    let indices = L.sort $ filter (".index" `L.isSuffixOf`) listing
    segments <- for (zip logs indices) $ \(logPath, idxPath) -> do
        let logOffset = Offset $ read $ dropExtension $ takeFileName logPath
        let idxOffset = Offset $ read $ dropExtension $ takeFileName idxPath
        unless (logOffset == idxOffset) exitFailure
        index <- loadIndex idxPath
        return Segment{initialOffset = logOffset, ..}
    return Topic{..}

-- | Load an index file completely into memory and parse it.
loadIndex :: FilePath -> IO Index
loadIndex idxPath =
    either (error "loadIndex: TODO") id . AP.parseOnly parseIndex <$>
      BS.readFile idxPath

-- | Read messages from the given topic, starting at the given offset.
readTopic :: (MonadSafe m) => Offset -> Topic -> Producer MessageEntry m ()
readTopic targetOffset topic = do
    let relevantSegments =
          dropUntilLastSuchThat ((<= targetOffset) . initialOffset) (segments topic)
    void $ mapM (readSegment targetOffset) relevantSegments

-- | Given a segment and an offset, return containi
readSegment :: (MonadSafe m) => Offset -> Segment -> Producer MessageEntry m ()
readSegment targetOffset Segment{..} = do
    let lowerBoundPos = lookupPosition (targetOffset .-. initialOffset) index
    readLog logPath lowerBoundPos >-> P.dropWhile ((< targetOffset) . offset)

-- | Read the given log file from the specified byte offset, and parse the
-- result as a sequence of `Message`s. Parse errors are raised as
-- exceptions. If the given offset doesn't correspond to a message
-- boundary, this will (likely) cause a parse error.
readLog
    :: (MonadSafe m) => FilePath -> LogPosition -> Producer MessageEntry m ()
readLog path (LogPosition offset) = decompressStream $ do
    let rawData = readFileFromOffset path (fromIntegral offset)
    ret <- PAP.parsed parseMessageEntry rawData
    liftIO $ either (throwIO . fst) return ret

-- | Read the given file from the specified byte offset.
readFileFromOffset
    :: (MonadSafe m) => FilePath -> Int -> Producer ByteString m ()
readFileFromOffset path offset =
    P.withFile path IO.ReadMode $ \handle -> do
      liftIO $ IO.hSeek handle IO.AbsoluteSeek (fromIntegral offset)
      PBS.fromHandle handle

-- | Returns a lower bound on the target message's byte position within
-- a segment. If the offset is negative, return zero. If the offset is
-- greater than the number of messages within the segment, return
-- a position close to the end of the segement.
lookupPosition
    :: RelativeOffset -- ^ Message offset relative to the start of the segment
    -> Index          -- ^ The index for the segment
    -> LogPosition
lookupPosition targetOffset index =
    maybe (LogPosition 0) logPosition $
        findLastSuchThat ((<= targetOffset) . relativeOffset) $
        IndexEntry (RelativeOffset 0) (LogPosition 0) : V.toList index


-------------------------------------------------------------------------------
-- Pretty-printing

-- | Pretty-print a message in the format
--
--     <offset>: [key]: <value>
ppMessage :: MessageEntry -> String
ppMessage MessageEntry{offset = Offset o, message = Message{..}} = concat
    [ show o <> ": "
    , case key   of Just k -> show k <> ": "; Nothing -> ""
    , case value of Just v -> show v; Nothing -> "<no value>"
    ]

