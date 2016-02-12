{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE LambdaCase #-}

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


-- | Snappy unfortunately has a variety of historical framing formats, and
-- while the comminity has now accepted "framing2" as the default, Kafka
-- still uses the "snappy-java" framing format.
data FramingFormat
    = Framing2      -- default        extension: sz
    | Framing1      -- obselete       extension: sz
    | SNZip         -- non-standard   extension: snz
    | SnappyJava    -- non-standard   extension: snappy
    | SnappyInJava  -- non-standard   extension: snappy
    | Comment43     -- obsolete       extension: snappy

decompress' :: (Monad m) => ByteString -> Producer ByteString m ()
decompress' bs = do
    AP.Done remainder header <- pure $ AP.parse parseHeader bs
    either (error err) id <$> parsed (parseBlock header) (yield remainder)
  where
    err = "Codec.Compression.Snappy.Extras.decompress': parse error"

-- | Parse the header which tells us what framing format we're using.
parseHeader :: Parser FramingFormat
parseHeader = msum
    [ Framing2     <$ header_Framing2
    , Framing1     <$ header_Framing1
    , SNZip        <$ header_SNZip
    , SnappyJava   <$ header_SnappyJava
    , SnappyInJava <$ header_SnappyInJava
    , Comment43    <$ header_Comment43
    ]

-- | Parse a single block of the compressed bytestream. Kafka always uses
-- "snappy-java"; therefore, it's the only format I've bothered to
-- implement a decoder for.
parseBlock :: FramingFormat -> Parser ByteString
parseBlock = \case
    Framing2     -> fail "Snappy.parseBlock: Framing2 not implemented"
    Framing1     -> fail "Snappy.parseBlock: Framing1 not implemented"
    SNZip        -> fail "Snappy.parseBlock: SNZip not implemented"
    SnappyJava   -> block_SnappyJava
    SnappyInJava -> fail "Snappy.parseBlock: SnappyInJava not implemented"
    Comment43    -> fail "Snappy.parseBlock: Comment43 not implemented"

-- Example encoding of the string "foobar\n" (no framing):
--
-- 00000000  07 18 66 6f 6f 62 61 72  0a                       |..foobar.|
-- 00000009

-------------------------------------------------------------------------------
-- framing2

-- Example encoding of the string "foobar\n":
--
-- 00000000  ff 06 00 00 73 4e 61 50  70 59 01 0b 00 00 0a 17  |....sNaPpY......|
-- 00000010  bb 3a 66 6f 6f 62 61 72  0a                       |.:foobar.|
-- 00000019
--
-- [1]: http://code.google.com/p/snappy/source/browse/trunk/framing_format.txt?r=71

header_Framing2 :: Parser ()
header_Framing2 =
    void $ AP.string "\xff\x06\x00\x00sNaPpY"

-------------------------------------------------------------------------------
-- framing1

-- Example encoding of the string "foobar\n":
--
-- 00000000  ff 06 00 73 4e 61 50 70  59 01 0b 00 0a 17 bb 3a  |...sNaPpY......:|
-- 00000010  66 6f 6f 62 61 72 0a                              |foobar.|
-- 00000017
--
-- [2]: http://code.google.com/p/snappy/source/browse/trunk/framing_format.txt?r=55

header_Framing1 :: Parser ()
header_Framing1 =
    void $ AP.string "\xff\x06\x00sNaPpY"

-------------------------------------------------------------------------------
-- snzip

-- Example encoding of the string "foobar\n":
--
-- 00000000  53 4e 5a 01 10 09 07 18  66 6f 6f 62 61 72 0a 00  |SNZ.....foobar..|
-- 00000010
--
-- [3]: https://github.com/kubo/snzip

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

-- Example encoding of the string "foobar\n":
--
-- 00000000  73 6e 61 70 70 79 00 00  00 07 3a bb 17 0a 66 6f  |snappy....:...fo|
-- 00000010  6f 62 61 72 0a                                    |obar.|
-- 00000015
--
-- [5]: https://github.com/dain/snappy

header_SnappyInJava :: Parser ()
header_SnappyInJava =
    void $ AP.string "snappy"

-------------------------------------------------------------------------------
-- comment-43

-- Example encoding of the string "foobar\n":
--
-- 00000000  ff 06 00 73 6e 61 70 70  79 01 0b 00 0a 17 bb 3a  |...snappy......:|
-- 00000010  66 6f 6f 62 61 72 0a fe  00 00                    |foobar....|
-- 0000001a
--
-- Reference: http://code.google.com/p/snappy/issues/detail?id=34#c43

header_Comment43 :: Parser ()
header_Comment43 =
    void $ AP.string "\xff\x06\x00snappy"
