{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}

module Database.Kafkalite.Backend.Tar
    ( TarM
    , runTarM
    ) where

import qualified Codec.Archive.Tar as Tar
import qualified Codec.Archive.Tar.Index as Tar
import Control.Monad.IO.Class
import Control.Monad.Reader
import qualified Data.ByteString.Lazy as BL
import qualified Data.List as L
import Data.Maybe
import Pipes
import qualified Pipes.ByteString as PBS
import Pipes.Safe
import qualified Pipes.Safe.Prelude as P
import qualified System.IO as IO

import Database.Kafkalite.Class
import Database.Kafkalite.Types

newtype TarM a = TarM (ReaderT (FilePath, Tar.TarIndex) (SafeT IO) a)
    deriving (Functor, Applicative, Monad, MonadIO, MonadThrow, MonadCatch, MonadMask)

runTarM
    :: TarM a
    -> FilePath       -- ^ Tarred Kafka log directory
    -> IO a
runTarM (TarM x) tarPath = do
    Right tarIndex <- Tar.build . Tar.read <$> BL.readFile tarPath
    runSafeT (runReaderT x (tarPath, tarIndex))

instance MonadSafe TarM where
    type Base TarM = IO
    liftBase = TarM . liftBase
    register = TarM . register
    release  = TarM . release

instance MonadKafka TarM where
    kafkaListDirectory fp = do
        entries <- Tar.toList <$> getTarIndex
        let relevantEntries = mapMaybe (L.stripPrefix (fp ++ "/") . fst) entries
        when (null relevantEntries) $ error "Tar.kafkaListDirectory: not found"
        return relevantEntries
    kafkaReadStrict fp = do
        tarIndex <- getTarIndex
        case Tar.lookup tarIndex fp of
          Just (Tar.TarFileEntry tarOffset) -> do
            tarPath <- getTarPath
            liftIO $ IO.withFile tarPath IO.ReadMode $ \h -> do
                entry <- Tar.hReadEntry h tarOffset
                case Tar.entryContent entry of
                  Tar.NormalFile content _size ->
                      return $ BL.toStrict content
                  _ -> error "Tar.kafkaReadStrict: not a file"
          _ -> error "Tar.kafkaReadStrict: not found"
    kafkaReadLazyFromPosition fp (FilePosition pos0) = do
        let pos = fromIntegral pos0
        Just (Tar.TarFileEntry tarOffset) <- flip Tar.lookup fp <$> lift getTarIndex
        tarPath <- lift getTarPath
        P.withFile tarPath IO.ReadMode $ \h -> do
            entry <- liftIO $ Tar.hReadEntryHeader h tarOffset
            case Tar.entryContent entry of
              Tar.NormalFile _content fileSize | pos <= fileSize -> do
                  liftIO $ IO.hSeek h IO.RelativeSeek (fromIntegral pos)
                  PBS.fromHandle h >-> PBS.take (fileSize - pos)
              _ -> error "Tar.kafkaReadLazyFromPosition: not a file"

getTarPath :: TarM FilePath
getTarPath = fst <$> TarM ask

getTarIndex :: TarM Tar.TarIndex
getTarIndex = snd <$> TarM ask
