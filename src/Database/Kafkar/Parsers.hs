{-# LANGUAGE RecordWildCards #-}

module Database.Kafkar.Parsers
    ( parseMessageEntry
    , parseIndex

    , kafkaBytes
    , kafkaString
    , kafkaArray
    ) where

import Control.Monad
import Data.Attoparsec.ByteString (Parser)
import qualified Data.Attoparsec.ByteString.Extras as AP
import Data.Bits
import Data.ByteString (ByteString)
import qualified Data.Vector.Unboxed as V

import Database.Kafkar.Types


-------------------------------------------------------------------------------
-- Message sets

parseMessageEntry :: Parser MessageEntry
parseMessageEntry = do
    offset <- Offset <$> AP.anyInt64be
    size <- fromIntegral <$> AP.anyInt32be
    message <- parseMessageV0 <|> parseMessageV1
    return MessageEntry{..}

-- v0
-- Message => Crc MagicByte Attributes Key Value
--   Crc => int32
--   MagicByte => int8
--   Attributes => int8
--   Key => bytes
--   Value => bytes
parseMessageV0 :: Parser Message
parseMessageV0 = do
    _crc <- AP.anyInt32be  -- TODO: check crc
    magic <- AP.anyInt8
    guard $ magic == 0
    attributes <- parseAttributes
    key <- kafkaBytes
    value <- kafkaBytes   -- TODO: make sure these are being evaluated strictly
    return MessageV0{..}

-- v1 (supported since 0.10.0)
-- Message => Crc MagicByte Attributes Key Value
--   Crc => int32
--   MagicByte => int8
--   Attributes => int8
--   Timestamp => int64
--   Key => bytes
--   Value => bytes
parseMessageV1 :: Parser Message
parseMessageV1 = do
    _crc <- AP.anyInt32be  -- TODO: check crc
    magic <- AP.anyInt8
    guard $ magic == 1
    attributes <- parseAttributes
    timestamp <- parseTimestamp
    key <- kafkaBytes
    value <- kafkaBytes   -- TODO: make sure these are being evaluated strictly
    return MessageV1{..}

-- This byte holds metadata attributes about the message. The lowest 2 bits
-- contain the compression codec used for the message. The other bits
-- should be set to 0.
parseAttributes :: Parser Attributes
parseAttributes = do
    attrs <- AP.anyInt8
    compression <- case attrs .&. 0x3 of  -- mask all but the lower 2 bits
          0 -> return None
          1 -> return GZip
          2 -> return Snappy
          _ -> fail "Unknown codec"
    return Attributes{..}

parseTimestamp :: Parser Timestamp
parseTimestamp =
    Timestamp <$> AP.anyInt64be

-------------------------------------------------------------------------------
-- Indices

parseIndex :: Parser Index
parseIndex =
    V.fromList <$> AP.many' parseIndexEntry

parseIndexEntry :: Parser IndexEntry
parseIndexEntry = do
    relativeOffset <- RelativeOffset <$> AP.anyInt32be
    logPosition <- LogPosition <$> AP.anyInt32be
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

{-# INLINE kafkaBytes #-}
kafkaBytes :: Parser (Maybe ByteString)
kafkaBytes = do
    len <- AP.anyInt32be
    if len == -1
      then return Nothing
      else Just <$> AP.take (fromIntegral len)

{-# INLINE kafkaString #-}
kafkaString :: Parser (Maybe ByteString)
kafkaString = do
    len <- AP.anyInt16be
    if len == -1
      then return Nothing
      else Just <$> AP.take (fromIntegral len)

{-# INLINE kafkaArray #-}
kafkaArray :: Parser a -> Parser [a]
kafkaArray inner = do
    len <- AP.anyInt32be
    replicateM (fromIntegral len) inner

-- References:
-- [0]: https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-Messagesets
-- [1]: https://stackoverflow.com/questions/19394669/why-index-file-exists-in-kafka-log-directory
