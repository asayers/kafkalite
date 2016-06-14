{-# LANGUAGE RecordWildCards #-}

-- | Kafka supports compressing messages for additional efficiency.
-- Multiple consecutive messages are compressed as a batch to improve the
-- compression ratio. (Of course, you may disable this behaviour by using
-- a batch size of 1.)
--
-- The producing client: A batch of uncompressed messages is concatenated
-- to form a MessageSet, which is then gzipped/snzipped and stored in the
-- Value field of a single "Message" with the appropriate compression codec
-- flag set. The offset of this Message should be the offset of the final
-- compressed message. (Note that this means that the offsets of the
-- messages in the top-level MessageSet are not dense.)
--
-- The server: The Kafka broker simply stores the data it recieves from the
-- producer.
--
-- The consuming client: When a client recieves a message which has a
-- compresseion flag set, it decompresses the value and parses it as
-- a MessageSet.
module Database.Kafkalite.Compression
    ( decompressStream
    ) where

import qualified Codec.Compression.GZip.Extras as G
import qualified Codec.Compression.Snappy.Framed as S
import Control.Monad.Catch
import Data.ByteString (ByteString)
import Data.Maybe
import Pipes

import Database.Kafkalite.Binary
import Database.Kafkalite.Stream
import Database.Kafkalite.Types

-- From the docs:
--
-- > Kafka supports compressing messages for additional efficiency, however
-- > this is more complex than just compressing a raw message. Because
-- > individual messages may not have sufficient redundancy to enable good
-- > compression ratios, compressed messages must be sent in special
-- > batches (although you may use a batch of one if you truly wish to
-- > compress a message on its own). The messages to be sent are wrapped
-- > (uncompressed) in a MessageSet structure, which is then compressed and
-- > stored in the Value field of a single "Message" with the appropriate
-- > compression codec set. The receiving system parses the actual
-- > MessageSet from the decompressed value. The outer MessageSet should
-- > contain only one compressed "Message" (see KAFKA-1718 for details).
--
-- https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-Compression

-- | Every message in the resulting stream will have `compression == None`.
decompressStream
    :: (MonadThrow m) => Producer MessageEntry m () -> Producer MessageEntry m ()
decompressStream input =
    for input decompressMsg

-- Note that this function is mutually recursive with `decompressStream`.
decompressMsg
    :: (MonadThrow m) => MessageEntry -> Producer MessageEntry m ()
decompressMsg entry =
    case compression (attributes $ message entry) of
        None   -> yield entry
        GZip   -> decompressStream $ decompressMsgWith G.decompress' entry
        Snappy -> decompressStream $ decompressMsgWith S.decompress_ entry

-- | Given a message which is wrapping a compressed collection of messages,
-- decompress the value using the provided function, then parse the result
-- as a sequence of messages and yield them as a Producer.
decompressMsgWith
    :: (MonadThrow m)
    => (ByteString -> Producer ByteString m ())    -- ^ Decompression function
    -> MessageEntry                  -- ^ Message wrapping compressed data
    -> Producer MessageEntry m ()    --   Inner messages
decompressMsgWith decompress =
    kdecode (getMessageEntry) . -- parse the result as a stream of messages
    decompress .                -- decompress it
    errIfNull .                 -- ... which shouldn't be empty
    value . message             -- extract the payload
  where
    errIfNull =
      fromMaybe (error "decompressMsg: compressed messages cannot be empty")
