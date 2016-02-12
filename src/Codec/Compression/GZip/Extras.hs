
module Codec.Compression.GZip.Extras
    ( module GZip
    , decompress'
    ) where

import Codec.Compression.GZip as GZip
import Data.ByteString (ByteString)
import qualified Data.ByteString.Lazy as BL
import Pipes
import qualified Pipes.ByteString as P

decompress' :: (Monad m) => ByteString -> Producer ByteString m ()
decompress' = P.fromLazy . GZip.decompress . BL.fromStrict
