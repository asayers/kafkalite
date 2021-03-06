{-# LANGUAGE RecordWildCards #-}

module Main where

import Database.Kafkalite
import Database.Kafkalite.Backend.FileSystem
import Options.Applicative
import Pipes
import qualified Pipes.Prelude as P


data Args = Args
    { kafkaDir :: FilePath
    , topicName :: String
    , partition :: Int
    , offset :: Int
    , maxMsgs :: Maybe Int
    }

argParser :: Parser Args
argParser = (<*>) helper $ Args
    <$> (strOption (long "log-dir" <> short 'd'
            <> help "The Kafka log directory. Defaults to /var/lib/kafka."
            <> metavar "DIR") <|> pure "/var/lib/kafka")
    <*> strOption (long "topic" <> short 't'
            <> help "The topic to stream from."
            <> metavar "STRING")
    <*> (option auto (long "partition" <> short 'p'
            <> help "The partition to stream from. Defaults to 0."
            <> metavar "INT") <|> pure 0)
    <*> (option auto (long "offset" <> short 'o'
            <> help "The starting offset. Defaults to 0."
            <> metavar "INT") <|> pure 0)
    <*> (Just <$> option auto (long "max" <> short 'm'
            <> help "Number of message to read. Defaults to unlimited."
            <> metavar "INT") <|> pure Nothing)

desc :: InfoMod Args
desc = fullDesc
    <> header "kafkalite - stream your kafka logs, no broker required!"

main :: IO ()
main = do
    Args{..} <- execParser (info argParser desc)
    topic <- runFileSystemM (loadTopic topicName partition) kafkaDir
    let pipeline = runEffect $
                readTopic topic (Offset $ fromIntegral offset)
            >-> maybe cat P.take maxMsgs
            >-> P.map ppMessage
            >-> P.stdoutLn
    runFileSystemM pipeline kafkaDir



