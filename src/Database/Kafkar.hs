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
-- == Usage
--
-- > import Database.Kafkar
-- > import Pipes
-- > import Pipes.Safe
-- > import qualified Pipes.Prelude as P
-- >
-- > main :: IO ()
-- > main = do
-- >     -- Read in and parse all index files for "myTopic"
-- >     topic <- loadTopic "/var/lib/kafka" "myTopic" 0
-- >
-- >     -- Read messages 1000-1009 from the topic. msgs :: [MessageEntry]
-- >     msgs <- runSafeT $ P.toList $
-- >         readTopic topic (Offset 1000) >-> P.take 10
-- >
-- >     -- Stream messages from the topic, starting at the beginning of
-- >     -- the stream, and pretty-print them to stdout
-- >     runSafeT $ runEffect $
-- >        readTopic topic (Offset 0) >-> P.map ppMessage >-> P.stdoutLn
--
-- == Operational properties
--
-- Memory use is constant-ish, but beware:
--
-- - In the case of uncompressed streams, it depends on the size of the
--   messages;
-- - In the case of compressed streams, it depends on the uncompressed size
--   of a batch of messages;
-- - All indices are loaded into memory. The amount of memory required for
--   this depends on the density of the indices (configurable) and the
--   total size of the stream in bytes. This behaviour could be improved by
--   only loading indices when seeking, and releasing them when the seek is
--   complete.
--
-- Files are opened only when needed and are closed promptly. When
-- streaming across a segment boundary, for instance, the first segment's
-- log file is closed as the second segment's log file is opened.
--
-- == Is this a good idea?
--
-- The Kafka on-disk format has never been advertised as a stable, portable
-- format. It is essentially undocumented; writing this package required
-- some mild reverse-engineering. In light of this, is it a good idea to
-- try to read these things directly?
--
-- Kafka makes the clever design decision of keeping data on-disk in
-- exactly the format which it must be sent in. That is, it sits on the
-- disk, compressed and annotated with metadata, all ready to be sent to
-- a client. This means that Kafka can respond to data requests with
-- a simple (and fast) call to `sendFile()`.
--
-- The upshot of this is that the on-disk format must be as stable as the
-- wire format. Since this is a well-documented, properly versioned format
-- which comes with commitments to backward-compatibility, it's safe to say
-- that the on-disk format is likewise stable (barring major changes to
-- Kafka's design).
--
-- I should note that the above argument applies only to the format of the
-- log data. The index files are not exposed by any public interface, and
-- so may change. In this case, seeking will break.
--
-- == Other caveats
--
-- - Kafka only flushes its logs to disk periodically. This means that new
--   messages in a topic will be absent from the on-disk logs for some
--   amount of time.
--
module Database.Kafkar
    ( -- * Core API
      loadTopic   -- :: FilePath -> String -> Int -> IO Topic
    , readTopic   -- :: Topic -> Offset -> Producer MessageEntry IO ()

      -- * Types
    , MessageEntry(..)
    , Message(..)
    , Attributes(..)
    , Codec(..)
    , Offset(..)

      -- * Pretty-printing
    , ppMessage   -- :: MessageEntry -> String
    ) where

import Control.Exception (throwIO)
import Control.Monad
import Control.Monad.IO.Class
import Data.AffineSpace
import Data.AdditiveGroup
import qualified Data.Attoparsec.ByteString as AP
import Data.ByteString (ByteString)
import qualified Data.ByteString as BS
import Data.Function
import qualified Data.List as L
import Data.Maybe
import Data.Monoid
import Data.Traversable
import qualified Data.Vector as VB
import Pipes hiding (for)
import Pipes.Safe
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


-- | Create a handle for a topic/partition stored on the filesystem.
--
-- This function will read all index files for the given topic into memory
-- (~10KB per segment). It will not read any log data. The returned value
-- should not contain any thunks.
loadTopic
    :: FilePath  -- ^ Kafka log directory (eg. \/var\/lib\/kafka)
    -> String    -- ^ Topic name
    -> Int       -- ^ Partition number
    -> IO Topic
loadTopic logDir topicName partition = do
    let topicDir = logDir </> topicName ++ "-" ++ show partition
    listing <- map (topicDir </>) <$> listDirectory topicDir
    let logs    = L.sort $ filter (".log"   `L.isSuffixOf`) listing
    let indices = L.sort $ filter (".index" `L.isSuffixOf`) listing
    rawSegments <- for (zip logs indices) $ \(logPath, idxPath) -> do
        let logOffset = Offset $ read $ dropExtension $ takeFileName logPath
        let idxOffset = Offset $ read $ dropExtension $ takeFileName idxPath
        unless (logOffset == idxOffset) exitFailure
        index <- loadIndex idxPath
        return $! Segment{initialOffset = logOffset, ..}
    let segments = VB.fromList $ L.sortBy (compare `on` initialOffset) rawSegments
    return Topic{..}

-- | Load an index file completely into memory and parse it.
loadIndex :: FilePath -> IO Index
loadIndex idxPath =
    either (error "loadIndex: TODO") id . AP.parseOnly parseIndex <$>
      BS.readFile idxPath

-- | Stream messages from the given topic, starting at the given offset.
--
-- This function uses the Kafka index to perform an efficient seek to the
-- desired offset. Given that the index is in memory, a seek should require
-- reading a bounded number of bytes (determined by the density of the
-- index).
--
-- It then streams messages from the log file, parsing and decompressing
-- them. The `compression` attribute should always be `None` for messages
-- returned by this function.
--
-- Kafka logs are stored in multiple segments. This function should only
-- hold a read handle for one segment file at a time.
readTopic
    :: (MonadSafe m)
    => Topic      -- ^ Topic handle to read from
    -> Offset     -- ^ Offset to begin reading at, given as a number of
                  --   messages since the start of the topic.
    -> Producer MessageEntry m ()
readTopic topic targetOffset = do
    let (logFiles, startPos, remainingMsgs) = lookupPosition targetOffset topic
    let positions = startPos : repeat (LogPosition 0)
    let msgsFromStartPos =
          mapM_ (uncurry readLog) (zip (VB.toList logFiles) positions)
    msgsFromStartPos >-> dropMessages remainingMsgs

dropMessages :: (Monad m) => RelativeOffset -> Pipe MessageEntry MessageEntry m ()
dropMessages (RelativeOffset offset) = P.drop (fromIntegral offset)

-- | Read the given log file from the specified byte offset, and parse the
-- result as a sequence of `Message`s. Parse errors are raised as
-- exceptions. If the given offset doesn't correspond to a message
-- boundary, this will (likely) cause a parse error.
--
-- The resulting messages have `compression = None`, and their offsets are
-- sequential and dense.
readLog
    :: (MonadSafe m) => FilePath -> LogPosition -> Producer MessageEntry m ()
readLog path offset =
    decompressStream $ readLogCompressed path offset

-- The resulting message may have a mix of compression attributes, and
-- their offsets are in-order but not dense. The offset of a compressed
-- message indicates the offset of the *last* message stored in its value
-- field.
readLogCompressed
    :: (MonadSafe m) => FilePath -> LogPosition -> Producer MessageEntry m ()
readLogCompressed path (LogPosition offset) = do
    let rawData = readFileFromOffset path (fromIntegral offset)
    ret <- PAP.parsed parseMessageEntry rawData
    liftIO $ either (throwIO . fst) return ret

-- | Read the given file from the specified byte offset.
readFileFromOffset
    :: (MonadSafe m) => FilePath -> Int -> Producer ByteString m ()
readFileFromOffset path offset =
    P.withFile path IO.ReadMode $ \h -> do
      liftIO $ IO.hSeek h IO.AbsoluteSeek (fromIntegral offset)
      PBS.fromHandle h

-------------------------------------------------------------------------------
-- Reading the index

-- | TODO
lookupPosition
    :: Offset   -- ^ The target message
    -> Topic    -- ^ The topic
    -> ( VB.Vector FilePath  --- ^ Paths of the relevant log files
       , LogPosition         --- ^ Starting position within the first file
       , RelativeOffset      --- ^ Number of messages to skip after seeking
       )
lookupPosition targetOffset Topic{..} =
    let relevantSegments =
          dropUntilLast ((<= targetOffset) . initialOffset) segments
        (position, remainder) = maybe (LogPosition 0, targetOffset .-. Offset 0)
          (lookupPositionSegment targetOffset) (relevantSegments VB.!? 0)
    in (VB.map logPath relevantSegments, position, remainder)

-- | Returns a lower bound on the target message's byte position within
-- a segment's log file. If the offset is negative, return zero. If the
-- offset is greater than the number of messages within the segment, return
-- a position close to the end of the segement.
lookupPositionSegment :: Offset -> Segment -> (LogPosition, RelativeOffset)
lookupPositionSegment targetOffset Segment{..} =
    lookupPositionIndex (targetOffset .-. initialOffset) index

-- | Returns the byte position of some message within the
-- corresponding log file, and the offset by which this message precedes
-- the target.
--
-- - If the target message is contained in this segment, then
--   this offset will be non-negative and small.
-- - If the target is in an earlier segment, the position will be zero and
--   the offset will be negative.
-- - If the target is in a later segment, the position will be somewhere
--   near the end of the log file and the offset will likely be large.
lookupPositionIndex :: RelativeOffset -> Index -> (LogPosition, RelativeOffset)
lookupPositionIndex targetOffset index =
    fromMaybe (LogPosition 0, targetOffset) $ do
        entry <- findLastInit ((<= targetOffset) . relativeOffset) index
        return (logPosition entry, targetOffset ^-^ relativeOffset entry)


-------------------------------------------------------------------------------
-- Pretty-printing

-- | Pretty-print a message in the format
--
-- > <offset>: [key:] <value>
ppMessage :: MessageEntry -> String
ppMessage MessageEntry{offset = Offset o, message = Message{..}} = concat
    [ show o <> ": "
    , case key   of Just k -> show k <> ": "; Nothing -> ""
    , case value of Just v -> show v; Nothing -> "<no value>"
    ]

