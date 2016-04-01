{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE TemplateHaskell #-}

module Database.Kafkar.Types
    ( -- * Topics
      Topic(..)
    , Segment(..)

      -- * Message sets
    , MessageEntry(..)
    , Message(..)
    , Attributes(..)

      -- * Indices
    , Index
    , IndexEntry(..)

      -- * Primitives
    , Codec(..)
    , Offset(..)
    , RelativeOffset(..)
    , LogPosition(..)
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
newtype Offset = Offset Int64
    deriving (Eq, Show, Ord)

-- | The difference between two logical message offsets
newtype RelativeOffset = RelativeOffset Int32
    deriving (Eq, Show, Ord)

-- | The byte-offset of a message within a log file
newtype LogPosition = LogPosition Int32
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
    , logPosition    :: {-# UNPACK #-} !LogPosition
        -- ^ The byte position in the corresponding log file
    } deriving (Eq, Show)

derivingUnbox "IndexEntry"
    [t| IndexEntry -> (Int32,Int32) |]
    [| \(IndexEntry (RelativeOffset x) (LogPosition y)) -> (x,y) |]
    [| \(x, y) -> IndexEntry (RelativeOffset x) (LogPosition y) |]

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

data Message = Message
    { attributes :: {-# UNPACK #-} !Attributes
    , key        ::                !(Maybe ByteString)
    , value      ::                !(Maybe ByteString)
    } deriving (Eq, Show)

data Attributes = Attributes
    { compression :: !Codec
    } deriving (Eq, Show)

-------------------------------------------------------------------------------
-- Topics

data Topic = Topic
    { topicName :: !String
    , partition :: !Int
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
