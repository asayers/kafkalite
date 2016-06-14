
module Database.Kafkalite.Class
    ( MonadKafka(..)
    ) where

import Data.ByteString (ByteString)
import Pipes

import Database.Kafkalite.Types

-- | A handle to the kafka data store. This could be files on the local
-- filesystem, in a tarball, over SSH, etc.
class Monad m => MonadKafka m where
    kafkaListDirectory        :: FilePath -> m [FilePath]
    kafkaReadStrict           :: FilePath -> m ByteString

    -- | Read the given file from the specified byte offset.
    kafkaReadLazyFromPosition :: FilePath -> FilePosition -> Producer ByteString m ()

