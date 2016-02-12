
module Database.Kafkar.Reader
    (
    ) where



data FileStore
    = FileSystem {- root dir -} FilePath
    | TarFile {- path to file -} FilePath

readFromStore
    :: (MonadSafe m)
    => FileStore -> FilePath -> LogPosition -> Producer ByteString m ()
readFromStore (FileSystem path) = readFromFileSystem path
readFromStore (TarFile path) = readFromTarFile path

readFromFileSystem
    :: (MonadSafe m)
    => FilePath -> FilePath -> LogPosition -> Producer ByteString m ()
readFromFileSystem root file offset =
    P.withFile (root </> file) IO.ReadMode $ \handle -> do
      liftIO $ IO.hSeek handle IO.AbsoluteSeek (fromIntegral offset)
      PBS.fromHandle handle

readFromTarFile
    :: (MonadSafe m)
    => FilePath -> FilePath -> LogPosition -> Producer ByteString m ()
readFromTarFile tarFile innerFile offset =
    P.withFile tarFile IO.ReadMode $ \handle -> do

      liftIO $ IO.hSeek handle IO.AbsoluteSeek (fromIntegral offset)
      PBS.fromHandle handle
