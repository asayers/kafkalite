
module Data.Attoparsec.ByteString.Extras
    ( module Data.Attoparsec.ByteString
    , module Data.Attoparsec.Binary

    , anyInt8
    , anyInt16be
    , anyInt32be
    , anyInt64be
    ) where

import Data.Attoparsec.ByteString
import Data.Attoparsec.Binary
import Data.Int

{-# INLINE anyInt8 #-}
anyInt8 :: Parser Int8
anyInt8 = fromIntegral <$> anyWord8

{-# INLINE anyInt16be #-}
anyInt16be :: Parser Int16
anyInt16be = fromIntegral <$> anyWord16be

{-# INLINE anyInt32be #-}
anyInt32be :: Parser Int32
anyInt32be = fromIntegral <$> anyWord32be

{-# INLINE anyInt64be #-}
anyInt64be :: Parser Int64
anyInt64be = fromIntegral <$> anyWord64be

