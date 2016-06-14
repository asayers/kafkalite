{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}

module Database.Kafkalite.Backend.FileSystem
    ( FileSystemM
    , runFileSystemM
    ) where

import Control.Monad.IO.Class
import Control.Monad.Reader
import qualified Data.ByteString as BS
import qualified Pipes.ByteString as PBS
import Pipes.Safe
import qualified Pipes.Safe.Prelude as P
import System.Directory
import System.FilePath
import qualified System.IO as IO

import Database.Kafkalite.Class
import Database.Kafkalite.Types

newtype FileSystemM a = FileSystemM (ReaderT FilePath (SafeT IO) a)
    deriving (Functor, Applicative, Monad, MonadIO, MonadThrow, MonadCatch, MonadMask)

runFileSystemM
    :: FileSystemM a
    -> FilePath       -- ^ Kafka log directory (eg. \/var\/lib\/kafka)
    -> IO a
runFileSystemM (FileSystemM x) baseDir = runSafeT (runReaderT x baseDir)

instance MonadSafe FileSystemM where
    type Base FileSystemM = IO
    liftBase = FileSystemM . liftBase
    register = FileSystemM . register
    release  = FileSystemM . release

instance MonadKafka FileSystemM where
    kafkaListDirectory fp = do
        baseDir <- getBaseDir
        liftIO $ listDirectory (baseDir </> fp)
    kafkaReadStrict fp = do
        baseDir <- getBaseDir
        liftIO $ BS.readFile (baseDir </> fp)
    kafkaReadLazyFromPosition fp (FilePosition pos) = do
        baseDir <- lift getBaseDir
        P.withFile (baseDir </> fp) IO.ReadMode $ \h -> do
          liftIO $ IO.hSeek h IO.AbsoluteSeek (fromIntegral pos)
          PBS.fromHandle h

getBaseDir :: FileSystemM FilePath
getBaseDir = FileSystemM ask
