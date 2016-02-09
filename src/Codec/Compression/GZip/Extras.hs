
module Codec.Compression.GZip.Extras
    ( module GZip
    , decompress'
    ) where

import Codec.Compression.GZip as GZip
import qualified Data.ByteString.Lazy as BL
import qualified Data.ByteString      as BS

decompress' :: BS.ByteString -> BS.ByteString
decompress' = BL.toStrict . GZip.decompress . BL.fromStrict
