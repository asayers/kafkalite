{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE LambdaCase #-}

module Database.Kafkalite.Types
    ( -- * Topics
      Topic(..)
    , TopicName
    , Partition
    , Segment(..)

      -- * Message sets
    , MessageEntry(..)
    , Message(..), attributes, timestamp, key, value
    , MessageV0(..), MessageV1(..)
    , Attributes(..)

      -- * Indices
    , Index
    , IndexEntry(..)

      -- * Primitives
    , Codec(..)
    , Offset(..)
    , RelativeOffset(..)
    , FilePosition(..)
    , Timestamp(..)
    , TimestampType(..)
    ) where

import Data.AdditiveGroup
import Data.AffineSpace
import Data.ByteString (ByteString)
import Data.Int
import qualified Data.Vector as VB
import qualified Data.Vector.Unboxed as VU
import Data.Vector.Unboxed.Deriving


-------------------------------------------------------------------------------
-- Primitives

-- | Message compression algorithm
data Codec = None | GZip | Snappy
    deriving (Eq, Show)

-- | The logical offset of a message within a topic
newtype Offset = Offset { unOffset :: Int64 }
    deriving (Eq, Show, Ord)

-- | The difference between two logical message offsets
newtype RelativeOffset = RelativeOffset { unRelativeOffset :: Int32 }
    deriving (Eq, Show, Ord)

-- | The byte-offset of a message within a log file
newtype FilePosition = FilePosition { unFilePosition :: Int32 }
    deriving (Eq, Show, Ord)

-- | The timestamp of a message, in milliseconds since beginning of the epoch
newtype Timestamp = Timestamp { unTimestamp :: Int64 }
    deriving (Eq, Show, Ord)

-- | The event which a timestamp records
data TimestampType = CreateTime | LogAppendTime
    deriving (Eq, Show, Ord)

instance AdditiveGroup RelativeOffset where
    (RelativeOffset x) ^+^ (RelativeOffset y) = RelativeOffset (x + y)
    zeroV = RelativeOffset 0
    negateV (RelativeOffset x) = RelativeOffset (negate x)

instance AffineSpace Offset where
    type Diff Offset = RelativeOffset
    (Offset x) .-. (Offset y) = RelativeOffset $ fromIntegral (x - y)
    (Offset x) .+^ (RelativeOffset y) = Offset (x + fromIntegral y)

-------------------------------------------------------------------------------
-- Indices

-- | Relates a logical message offset (given relative to the start of the
-- segment) to a byte offset within the log file for the segment.
data IndexEntry = IndexEntry
    { relativeOffset :: {-# UNPACK #-} !RelativeOffset
    , filePosition   :: {-# UNPACK #-} !FilePosition
        -- ^ The byte position in the corresponding log file
    } deriving (Eq, Show)

derivingUnbox "IndexEntry"
    [t| IndexEntry -> (Int32,Int32) |]
    [| \(IndexEntry (RelativeOffset x) (FilePosition y)) -> (x,y) |]
    [| \(x, y) -> IndexEntry (RelativeOffset x) (FilePosition y) |]

-- | An unboxed vector of offset-position relations, which is
--
-- - sorted in both columns; and
-- - sparse in both columns.
type Index = VU.Vector IndexEntry

-------------------------------------------------------------------------------
-- Message sets

data MessageEntry = MessageEntry
    { offset  :: {-# UNPACK #-} !Offset
    , size    :: {-# UNPACK #-} !Int32
    , message ::                !Message
    } deriving (Eq, Show)

data Message
    = MV0 MessageV0
    | MV1 MessageV1
    deriving (Eq, Show)

data MessageV0 = MessageV0
    { mv0Attributes :: {-# UNPACK #-} !Attributes
    , mv0Key        ::                !(Maybe ByteString)
    , mv0Value      ::                !(Maybe ByteString)
    } deriving (Eq, Show)

data MessageV1 = MessageV1
    { mv1Attributes :: {-# UNPACK #-} !Attributes
    , mv1Timestamp  :: {-# UNPACK #-} !Timestamp
    , mv1Key        ::                !(Maybe ByteString)
    , mv1Value      ::                !(Maybe ByteString)
    } deriving (Eq, Show)

attributes :: Message -> Attributes
timestamp  :: Message -> Maybe Timestamp
key        :: Message -> Maybe ByteString
value      :: Message -> Maybe ByteString

attributes = \case MV0 x -> mv0Attributes x ; MV1 x -> mv1Attributes x
timestamp  = \case MV0 _ -> Nothing         ; MV1 x -> Just (mv1Timestamp x)
key        = \case MV0 x -> mv0Key x        ; MV1 x -> mv1Key x
value      = \case MV0 x -> mv0Value x      ; MV1 x -> mv1Value x

data Attributes = Attributes
    { compression   :: !Codec
    , timestampType :: !TimestampType
    } deriving (Eq, Show)

-------------------------------------------------------------------------------
-- Topics

type TopicName = String
type Partition = Int

data Topic = Topic
    { topicName :: !TopicName
    , partition :: !Partition
    , segments  :: !(VB.Vector Segment)  -- ^ Segments sorted by initial offset
    } deriving (Eq, Show)

-- | Kafka streams are split into segments on disk. Each segment comprises
-- a log file, which stores the data, and an index, which allows fast
-- seeking to a specific message within the log file. The files associated
-- with a segment are named according to the offset (within the whole
-- topic) of the first message in the segment. The message offsets used in
-- the index are given relative to the starting offset of the segment.
data Segment = Segment
    { initialOffset :: !Offset
    , index         :: !Index
    , logPath       :: !FilePath
    } deriving (Eq, Show)
