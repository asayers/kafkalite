{-# LANGUAGE RecordWildCards #-}

module Database.Kafkar.Parsers
    ( parseMessageEntry
    , parseIndex
    ) where

import Control.Monad
import Data.Attoparsec.ByteString (Parser)
import qualified Data.Attoparsec.ByteString as AP
import qualified Data.Attoparsec.Binary as AP
import Data.Bits
import Data.ByteString (ByteString)
import Data.Int
import qualified Data.Vector as V

import Database.Kafkar.Types


-------------------------------------------------------------------------------
-- Message sets

parseMessageEntry :: Parser MessageEntry
parseMessageEntry = do
    offset <- Offset <$> anyInt64be
    size <- fromIntegral <$> anyInt32be
    message <- parseMessage
    return MessageEntry{..}

-- Message => Crc MagicByte Attributes Key Value
--   Crc => int32
--   MagicByte => int8
--   Attributes => int8
--   Key => bytes
--   Value => bytes
parseMessage :: Parser Message
parseMessage = do
    _crc <- anyInt32be  -- TODO: check crc
    magic <- anyInt8
    guard $ magic == 0    -- We only support version 0
    attributes <- parseAttributes
    key <- anyBytes
    value <- anyBytes
    return Message{..}

-- This byte holds metadata attributes about the message. The lowest 2 bits
-- contain the compression codec used for the message. The other bits
-- should be set to 0.
parseAttributes :: Parser Attributes
parseAttributes = do
    attrs <- anyInt8
    compression <- case attrs .&. 0x3 of  -- lower 2 bits
          0 -> return None
          1 -> return GZIP
          2 -> return Snappy
          _ -> fail "Unknown codec"
    return Attributes{..}

-------------------------------------------------------------------------------
-- Indices

parseIndex :: Parser Index
parseIndex =
    V.fromList <$> AP.many' parseIndexEntry

parseIndexEntry :: Parser IndexEntry
parseIndexEntry = do
    relativeOffset <- RelativeOffset <$> anyInt32be
    logPosition <- LogPosition <$> anyInt32be
    guard $ relativeOffset /= RelativeOffset 0  -- Otherwise we're at the end
    return IndexEntry{..}

-------------------------------------------------------------------------------
-- Protocol Primitive Types

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

{-# INLINE anyInt8 #-}
anyInt8 :: Parser Int8
anyInt8 = fromIntegral <$> AP.anyWord8

{-# INLINE anyInt16be #-}
anyInt16be :: Parser Int16
anyInt16be = fromIntegral <$> AP.anyWord16be

{-# INLINE anyInt32be #-}
anyInt32be :: Parser Int32
anyInt32be = fromIntegral <$> AP.anyWord32be

{-# INLINE anyInt64be #-}
anyInt64be :: Parser Int64
anyInt64be = fromIntegral <$> AP.anyWord64be

{-# INLINE anyBytes #-}
anyBytes :: Parser (Maybe ByteString)
anyBytes = do
    len <- anyInt32be
    if len == -1
      then return Nothing
      else Just <$> AP.take (fromIntegral len)

{-# INLINE anyString #-}
anyString :: Parser (Maybe ByteString)
anyString = do
    len <- anyInt16be
    if len == -1
      then return Nothing
      else Just <$> AP.take (fromIntegral len)

{-# INLINE anyArray #-}
anyArray :: Parser a -> Parser [a]
anyArray inner = do
    len <- anyInt32be
    replicateM (fromIntegral len) inner

-- References:
-- [0]: https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-Messagesets
-- [1]: https://stackoverflow.com/questions/19394669/why-index-file-exists-in-kafka-log-directory
