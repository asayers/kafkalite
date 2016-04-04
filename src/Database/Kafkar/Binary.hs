{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE LambdaCase #-}
{-# OPTIONS_GHC -fno-warn-orphans #-}

module Database.Kafkar.Binary
    ( getMessageEntry
    , getIndex
    ) where

import Control.Applicative
import Control.Monad
import Data.Binary
import Data.Binary.Get
import Data.Bits
import Data.ByteString (ByteString)
import qualified Data.ByteString as BS
import Data.Int
import qualified Data.Vector.Unboxed as VU

import Database.Kafkar.Types

-- References:
-- [0]: https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-Messagesets
-- [1]: https://stackoverflow.com/questions/19394669/why-index-file-exists-in-kafka-log-directory

-------------------------------------------------------------------------------
-- Message sets

getMessageEntry :: Get MessageEntry
getMessageEntry = do
    offset <- getOffset
    size <- fromIntegral <$> getInt32be
    message <- getMessage
    return MessageEntry{..}

getMessage :: Get Message
getMessage = msum
    [ MV0 <$> getMessageV0
    , MV1 <$> getMessageV1
    ]

-- v0
-- Message => Crc MagicByte Attributes Key Value
--   Crc => int32
--   MagicByte => int8
--   Attributes => int8
--   Key => bytes
--   Value => bytes
getMessageV0 :: Get MessageV0
getMessageV0 = do
    _crc <- getInt32be  -- TODO: check crc
    requireMagicByte 0
    mv0Attributes <- getAttributes
    mv0Key <- getKafkaBytes
    mv0Value <- getKafkaBytes   -- TODO: make sure these are being evaluated strictly
    return MessageV0{..}

-- Message => Crc MagicByte Attributes Key Value
--   Crc => int32
--   MagicByte => int8
--   Attributes => int8
--   Timestamp => int64
--   Key => bytes
--   Value => bytes
getMessageV1 :: Get MessageV1
getMessageV1 = do
    _crc <- getInt32be  -- TODO: check crc
    requireMagicByte 1
    mv1Attributes <- getAttributes
    mv1Timestamp <- getTimestamp
    mv1Key <- getKafkaBytes
    mv1Value <- getKafkaBytes   -- TODO: make sure these are being evaluated strictly
    return MessageV1{..}

-- This byte holds metadata attributes about the message. The lowest 2 bits
-- contain the compression codec used for the message. The other bits
-- should be set to 0.
getAttributes :: Get Attributes
getAttributes = do
    attrs <- getInt8
    compression <- case attrs .&. 0x3 of  -- mask all but the lower 2 bits
        0 -> return None
        1 -> return GZip
        2 -> return Snappy
        _ -> fail "Unknown codec"
    return Attributes{..}

getOffset :: Get Offset
getOffset = Offset <$> getInt64be

getTimestamp :: Get Timestamp
getTimestamp = Timestamp <$> getInt64be

-------------------------------------------------------------------------------
-- Indices

getIndex :: Get Index
getIndex = VU.fromList <$> many getIndexEntry

getIndexEntry :: Get IndexEntry
getIndexEntry = do
    relativeOffset <- RelativeOffset <$> getInt32be
    filePosition <- FilePosition <$> getInt32be
    guard $ relativeOffset /= RelativeOffset 0  -- Otherwise we're at the end
    return IndexEntry{..}

-------------------------------------------------------------------------------
-- Kafka protocol primitive types

-- The protocol is built out of the following primitive types.
--
-- - Fixed Width Primitives: int8, int16, int32, int64 - Signed integers
--   with the given precision (in bits) stored in big endian order.
-- - Variable Length Primitives: bytes, string - These types consist of
--   a signed integer giving a length N followed by N bytes of content.
--   A length of -1 indicates null. string uses an int16 for its size, and
--   bytes uses an int32.
-- - Arrays: This is a notation for handling repeated structures. These
--   will always be encoded as an int32 size containing the length
--   N followed by N repetitions of the structure which can itself be made
--   up of other primitive types. In the BNF grammars below we will show an
--   array of a structure foo as [foo].

{-# INLINE getKafkaBytes #-}
getKafkaBytes :: Get (Maybe ByteString)
getKafkaBytes = do
    len <- getInt32be
    if len == -1
      then return Nothing
      else Just <$> getByteString (fromIntegral len)

requireMagicByte :: Int8 -> Get ()
requireMagicByte magic =
    guard . (== magic) =<< getInt8
