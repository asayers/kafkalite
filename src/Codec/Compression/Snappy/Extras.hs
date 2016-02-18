{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE LambdaCase #-}

-- | Snappy is a block compression format, meaning that the whole
-- compressed stream must be kept in memory until it is fully decoded [1].
-- The task of splitting data into a stream of independently-decodable
-- chunks is handled by the framing format. In addition, this format often
-- provides checksums of the uncompressed data.
--
-- Unfortunately, for a long time snappy had no official framing format,
-- and so a number of improvised formats appeared. While there is now
-- a standard format, many of the historical formats are still in common
-- use. The good news is that these formats thankfully begin with distinct
-- magic byte sequences, and so can be easily distinguished.
--
-- This module provides facilities for decoding framed Snappy streams.
--
--
-- [1]: In Snappy, the offsets used by back-references may be as large as
-- a 32-bit word. As a result, a byte in the uncompressed stream can't be
-- discarded until 4GB of uncompressed data following it has been decoded.
-- This effectively makes Snappy a block compression format.
--
-- TODO (asayers): Finish this and release, either in `snappy` or in its
-- own package.
-- TODO (asayers): Tests
module Codec.Compression.Snappy.Extras
    ( module Snappy
    , decompress'
    ) where

import Codec.Compression.Snappy as Snappy
import Control.Monad
import Data.Attoparsec.ByteString (Parser)
import qualified Data.Attoparsec.ByteString.Extras as AP
import Data.ByteString (ByteString)
import Pipes
import Pipes.Attoparsec


-- | Decompress a framed Snappy stream
--
-- TODO (asayers): we can do better in terms of streaming the input.
decompress' :: (Monad m) => ByteString -> Producer ByteString m ()
decompress' bs = do
    AP.Done remainder header <- pure $ AP.parse parseHeader bs
    either (error err) id <$> parsed (parseBlock header) (yield remainder)
  where
    err = "Codec.Compression.Snappy.Extras.decompress': parse error"

-- | Snappy unfortunately has a variety of historical framing formats, and
-- while the comminity has now accepted "framing2" as the default, Kafka
-- still uses the "snappy-java" framing format.
data FramingFormat
    = Framing2      -- default        extension: sz
    | Framing1      -- obselete       extension: sz
    | SNZip         -- obsolete       extension: snz
    | SnappyJava    -- non-standard   extension: snappy
    | SnappyInJava  -- obsolete       extension: snappy
    | Comment43     -- obsolete       extension: snappy

-- | Parse the header which tells us which framing format we're using.
parseHeader :: Parser FramingFormat
parseHeader = msum
    [ Framing2     <$ header_Framing2
    , Framing1     <$ header_Framing1
    , SNZip        <$ header_SNZip
    , SnappyJava   <$ header_SnappyJava
    , SnappyInJava <$ header_SnappyInJava
    , Comment43    <$ header_Comment43
    ]

-- | Parse a single block of the compressed bytestream, returning a segment
-- of the uncompressed stream.
--
-- Kafka always uses "snappy-java"; therefore, it's the only format I've
-- bothered to implement a decoder for.
parseBlock :: FramingFormat -> Parser ByteString
parseBlock = \case
    Framing2     -> fail "Snappy.parseBlock: Framing2 not implemented"
    Framing1     -> fail "Snappy.parseBlock: Framing1 not implemented"
    SNZip        -> fail "Snappy.parseBlock: SNZip not implemented"
    SnappyJava   -> block_SnappyJava
    SnappyInJava -> fail "Snappy.parseBlock: SnappyInJava not implemented"
    Comment43    -> fail "Snappy.parseBlock: Comment43 not implemented"

-------------------------------------------------------------------------------
-- no framing

-- Example encoding of the string "foobar\n":
--
-- 00000000  07 18 66 6f 6f 62 61 72  0a                       |..foobar.|
-- 00000009

-- TODO (asayers): Correctly handle unframed snappy streams

-------------------------------------------------------------------------------
-- framing2

-- This covers the following revisions (the latest is backward-compatible
-- with the others):
--
-- - framing format revision 2013-10-25, snappy 1.1.2, svn r82, git f82bff6
-- - framing format revision 2013-01-05, snappy 1.1.0, svn r71, git 27a0cc3
--
-- Example encoding of the string "foobar\n":
--
-- 00000000  ff 06 00 00 73 4e 61 50  70 59 01 0b 00 00 0a 17  |....sNaPpY......|
-- 00000010  bb 3a 66 6f 6f 62 61 72  0a                       |.:foobar.|
-- 00000019
--
-- Reference: https://github.com/google/snappy/blob/f82bff6/framing_format.txt

header_Framing2 :: Parser ()
header_Framing2 =
    void $ AP.string "\xff\x06\x00\x00sNaPpY"

-------------------------------------------------------------------------------
-- framing1

-- This covers the following revision:
--
-- - framing format revision 2011-12-15, snappy 1.0.5, svn r55, git 0755c81
--
-- Example encoding of the string "foobar\n":
--
-- 00000000  ff 06 00 73 4e 61 50 70  59 01 0b 00 0a 17 bb 3a  |...sNaPpY......:|
-- 00000010  66 6f 6f 62 61 72 0a                              |foobar.|
-- 00000017
--
-- Reference: https://github.com/google/snappy/blob/0755c81/framing_format.txt

header_Framing1 :: Parser ()
header_Framing1 =
    void $ AP.string "\xff\x06\x00sNaPpY"

-------------------------------------------------------------------------------
-- snzip

-- From the docs:
--
-- > The first three bytes are magic characters 'SNZ'.
-- >
-- > The fourth byte is the file format version. It is 0x01.
-- >
-- > The fifth byte is the order of the block size. The input data is
-- > divided into fixed-length blocks and each block is compressed by
-- > snappy. When it is 16 (default value), the block size is 16th power of
-- > 2; 64 kilobytes.
-- >
-- > The rest is pairs of a compressed data length and a compressed data
-- > block The compressed data length is encoded as
-- > snappy::Varint::Encode32() does. If the length is zero, it is the end
-- > of data.
-- >
-- > Though the rest after the end of data is ignored for now, they may be
-- > continuously read as a next compressed file as gzip does.
-- >
-- > Note that the uncompressed length of each compressed data block must
-- > be less than or equal to the block size specified by the fifth byte.
--
-- Example encoding of the string "foobar\n":
--
-- 00000000  53 4e 5a 01 10 09 07 18  66 6f 6f 62 61 72 0a 00  |SNZ.....foobar..|
-- 00000010
--
-- Reference: https://github.com/kubo/snzip

header_SNZip :: Parser ()
header_SNZip =
    void $ AP.string "SNZ"

-------------------------------------------------------------------------------
-- snappy-java

-- From the docs:
--
-- > SnappyOutputStream and SnappyInputStream use `[magic header:16
-- > bytes]([block size:int32][compressed data:byte array])*` format
--
-- I'm assuming the int32 is big endian, given the example below.
--
-- Example encoding of the string "foobar\n"
--
-- 00000000  82 53 4e 41 50 50 59 00  00 00 00 01 00 00 00 01  |.SNAPPY.........|
-- 00000010  00 00 00 09 07 18 66 6f  6f 62 61 72 0a           |......foobar.|
-- 0000001d
--
-- Reference: http://code.google.com/p/snappy-java/

header_SnappyJava :: Parser ()
header_SnappyJava = do
    void $ AP.string "\x82SNAPPY\x00\x00\x00\x00\x01\x00\x00\x00\x01"

block_SnappyJava :: Parser ByteString
block_SnappyJava = do
    blockLen <- AP.anyInt32be
    blockData <- AP.take (fromIntegral blockLen)
    return $ decompress blockData

-------------------------------------------------------------------------------
-- snappy-in-java

-- From the docs:
--
-- > The output format is the stream header "snappy\0" followed by one or
-- > more compressed blocks of data, each of which is preceded by a seven
-- > byte header.
-- >
-- > The first byte of the header is a flag indicating if the block is
-- > compressed or not. A value of 0x00 means uncompressed, and 0x01 means
-- > compressed.
-- >
-- > The second and third bytes are the size of the block in the stream as
-- > a big endian number. This value is never zero as empty blocks are
-- > never written. The maximum allowed length is 32k (1 << 15).
-- >
-- > The remaining four byes are crc32c checksum of the user input data
-- > masked with the following function: {@code ((crc >>> 15) | (crc <<
-- > 17)) + 0xa282ead8 }
-- >
-- > An uncompressed block is simply copied from the input, thus
-- > guaranteeing that the output is never larger than the input (not
-- > including the header).
--
-- Example encoding of the string "foobar\n":
--
-- 00000000  73 6e 61 70 70 79 00 00  00 07 3a bb 17 0a 66 6f  |snappy....:...fo|
-- 00000010  6f 62 61 72 0a                                    |obar.|
-- 00000015
--
-- Reference: https://github.com/dain/snappy

header_SnappyInJava :: Parser ()
header_SnappyInJava =
    void $ AP.string "snappy\x00"

-------------------------------------------------------------------------------
-- comment-43

-- Example encoding of the string "foobar\n":
--
-- 00000000  ff 06 00 73 6e 61 70 70  79 01 0b 00 0a 17 bb 3a  |...snappy......:|
-- 00000010  66 6f 6f 62 61 72 0a fe  00 00                    |foobar....|
-- 0000001a
--
-- Reference: http://code.google.com/p/snappy/issues/detail?id=34#c43
--     (Broken link, not in the Wayback Machine. Lost to history?)

header_Comment43 :: Parser ()
header_Comment43 =
    void $ AP.string "\xff\x06\x00snappy"
