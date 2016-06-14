{-# LANGUAGE RecordWildCards #-}

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
-- == Compatibility
--
-- We currently support Kafka v0.8 and v0.9, as well as the current
-- development version of v0.10.
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
-- Performance is roughly half of what kafka delivers: on my machine, it
-- looks like I can get 850,000 messages/second with 0.03s constant
-- overhead. For comparison, streaming from kafka using kafkacat seems to
-- give 1,850,000 messages/second with 0.17s of constant overhead. This is
-- because we go through the trouble of pulling apart each message and
-- reassembling it on the heap. Not the best design for super-high
-- performance, but good enough for my purposes.
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
module Database.Kafkalite
    ( -- * Core API
      loadTopic   -- :: FilePath -> TopicName -> Partition -> IO Topic
    , readTopic   -- :: MonadSafe m => Topic -> Offset -> Producer MessageEntry m ()

      -- * Types
    , MonadKafka
    , TopicName
    , Partition
    , Offset(..)
    , MessageEntry(..)
    , Message(..)
    , Attributes(..)
    , Codec(..)

      -- * Pretty-printing
    , ppMessage   -- :: MessageEntry -> String
    ) where

import Control.Monad
import Data.AdditiveGroup
import Data.AffineSpace
import qualified Data.Binary.Get as B
import qualified Data.ByteString.Lazy as BL
import Data.Function
import qualified Data.List as L
import Data.Maybe
import Data.Monoid
import Data.Traversable
import qualified Data.Vector as VB
import Pipes hiding (for)
import qualified Pipes.Binary as P
import qualified Pipes.Prelude as P
import Pipes.Safe
import System.FilePath

import Database.Kafkalite.Binary
import Database.Kafkalite.Class
import Database.Kafkalite.Compression
import Database.Kafkalite.Stream
import Database.Kafkalite.Types
import Database.Kafkalite.Util


-- | Create a handle for a topic/partition stored on the filesystem.
--
-- This function will read all index files for the given topic into memory
-- (~100KB per segment). It will not read any log data. The returned value
-- should not contain any thunks.
loadTopic
    :: (MonadKafka m, MonadThrow m)
    => TopicName   -- ^ Topic name
    -> Partition   -- ^ Partition number
    -> m Topic
loadTopic topicName partition = do
    let topicDir = topicName ++ "-" ++ show partition
    listing <- map (topicDir </>) <$> kafkaListDirectory topicDir
    let logs    = L.sort $ filter (".log"   `L.isSuffixOf`) listing
    let indices = L.sort $ filter (".index" `L.isSuffixOf`) listing
    rawSegments <- for (zip logs indices) $ \(logPath, idxPath) -> do
        let logOffset = Offset $ read $ dropExtension $ takeFileName logPath
        let idxOffset = Offset $ read $ dropExtension $ takeFileName idxPath
        unless (logOffset == idxOffset) (error "log/index mismatch")
        index <- loadIndex idxPath
        return $! Segment{initialOffset = logOffset, ..}
    let segments = VB.fromList $ L.sortBy (compare `on` initialOffset) rawSegments
    return Topic{..}

-- | Load an index file completely into memory and parse it.
loadIndex :: (MonadKafka m, MonadThrow m) => FilePath -> m Index
loadIndex idxPath = do
    idxData <- BL.fromStrict <$> kafkaReadStrict idxPath
    case B.runGetOrFail getIndex idxData of
      Left  (_rem, offset, err) -> throwM (P.DecodingError offset err)
      Right (_rem,_offset, val) -> return val

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
    :: (MonadKafka m, MonadThrow m)
    => Topic      -- ^ Topic handle to read from
    -> Offset     -- ^ Offset to begin reading at, given as a number of
                  --   messages since the start of the topic.
    -> Producer MessageEntry m ()
readTopic topic targetOffset = do
    let (logFiles, startPos, remainingMsgs) =
            lookupPositionInTopic targetOffset topic
    let positions = startPos : repeat (FilePosition 0)
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
    :: (MonadKafka m, MonadThrow m)
    => FilePath -> FilePosition -> Producer MessageEntry m ()
readLog path offset =
    decompressStream $ readLogCompressed path offset

-- The resulting message may have a mix of compression attributes, and
-- their offsets are in-order but not dense. The offset of a compressed
-- message indicates the offset of the *last* message stored in its value
-- field.
readLogCompressed
    :: (MonadKafka m, MonadThrow m)
    => FilePath -> FilePosition -> Producer MessageEntry m ()
readLogCompressed path position =
    kdecode getMessageEntry (kafkaReadLazyFromPosition path position)


-------------------------------------------------------------------------------
-- Reading the index

-- | TODO
lookupPositionInTopic
    :: Offset   -- ^ The target message
    -> Topic    -- ^ The topic
    -> ( VB.Vector FilePath  --- ^ Paths of the relevant log files
       , FilePosition        --- ^ Starting position within the first file
       , RelativeOffset      --- ^ Number of messages to skip after seeking
       )
lookupPositionInTopic targetOffset Topic{..} =
    let relevantSegments =
          dropUntilLast ((<= targetOffset) . initialOffset) segments
        (position, remainder) = maybe (FilePosition 0, targetOffset .-. Offset 0)
          (lookupPositionInSegment targetOffset) (relevantSegments VB.!? 0)
    in (VB.map logPath relevantSegments, position, remainder)

-- | Returns the byte position of some message within the given segment,
-- and the offset by which this message precedes the target message.
--
-- The returned message is the closest indexed message in the segment which
-- precedes the target message.
--
-- - If the target message is contained in this segment, then
--   this offset will be non-negative and bounded in size.
-- - If the target is in an earlier segment, the position will be zero and
--   the offset will be negative and unbounded.
-- - If the target is in a later segment, the position will be somewhere
--   near the end of the segment's log file and the offset will be positive
--   and unbounded.
lookupPositionInSegment :: Offset -> Segment -> (FilePosition, RelativeOffset)
lookupPositionInSegment targetOffset Segment{..} =
    let targetRelOffset = targetOffset .-. initialOffset
    in fromMaybe (FilePosition 0, targetRelOffset) $ do
        entry <- findLastInit ((<= targetRelOffset) . relativeOffset) index
        return (filePosition entry, targetRelOffset ^-^ relativeOffset entry)


-------------------------------------------------------------------------------
-- Pretty-printing

-- | Pretty-print a message in the format
--
-- > <offset>: [key:] <value>
ppMessage :: MessageEntry -> String
ppMessage MessageEntry{offset = Offset o, message = m} = concat
    [ show o <> ": "
    , case key   m of Just k -> show k <> ": "; Nothing -> ""
    , case value m of Just v -> show v; Nothing -> "<no value>"
    ]

