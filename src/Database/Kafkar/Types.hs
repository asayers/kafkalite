{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE TypeFamilies #-}

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
import Data.Vector (Vector)


-------------------------------------------------------------------------------
-- Topics

data Topic = Topic
    { topicName :: !String
    , segments  :: ![Segment]
    } deriving (Eq, Show)

data Segment = Segment
    { initialOffset :: !Offset
    , index         :: !Index
    , logPath       :: !FilePath
    } deriving (Eq, Show)

-------------------------------------------------------------------------------
-- Message sets

data MessageEntry = MessageEntry
    { offset  :: {- UNPACK -} !Offset
    , size    :: {- UNPACK -} !Int32
    , message :: {- UNPACK -} !Message
    } deriving (Eq, Show)

data Message = Message
    { attributes :: {- UNPACK -} !Attributes
    , key        :: {- UNPACK -} !(Maybe ByteString)
    , value      :: {- UNPACK -} !(Maybe ByteString)
    } deriving (Eq, Show)

data Attributes = Attributes
    { compression :: {- UNPACK -} !Codec
    } deriving (Eq, Show)

-------------------------------------------------------------------------------
-- Indices

-- List of offset-position relations, which is
-- - sorted in both columns; and
-- - sparse in both columns.
type Index = Vector IndexEntry   -- TODO: unbox

-- Relates a logical message offset (given relative to the start of the
-- segment) to a byte offset within the corresponding log file.
data IndexEntry = IndexEntry
    { relativeOffset :: {- UNPACK -} !RelativeOffset
    , logPosition    :: {- UNPACK -} !LogPosition
        -- ^ The byte position in the corresponding log file
    } deriving (Eq, Show)

-------------------------------------------------------------------------------
-- Primitives

-- Message compression algorithm
data Codec = None | GZIP | Snappy
    deriving (Eq, Show)

-- The logical offset of a message within a topic
newtype Offset = Offset Int64
    deriving (Eq, Show, Ord)

-- The difference between two logical message offsets
newtype RelativeOffset = RelativeOffset Int32
    deriving (Eq, Show, Ord)

-- The byte-offset of a message within a log file
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
