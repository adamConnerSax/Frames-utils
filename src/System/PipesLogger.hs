{-# LANGUAGE OverloadedStrings #-}
module System.PipesLogger where

import           Control.Monad.IO.Class (MonadIO (..))
import           Data.Monoid            ((<>))
import qualified Data.Text              as T
import qualified Pipes                  as P


data LogSeverity = Info | Warning | Error deriving (Show, Eq, Ord)
data LogEntry = LogEntry { severity :: LogSeverity, message :: T.Text }

logEntryPretty :: LogEntry -> T.Text
logEntryPretty (LogEntry Info t)    = "Info: " <> t
logEntryPretty (LogEntry Warning w) = "Warning: " <> w
logEntryPretty (LogEntry Error  e)  = "Error: " <> e

filterLogEntry :: [LogSeverity] -> LogEntry -> Maybe LogEntry
filterLogEntry ls (LogEntry s m) = if (s `elem` ls) then Just (LogEntry s m) else Nothing

type Logger m = P.Producer LogEntry m
log :: Monad m => LogSeverity-> T.Text -> Logger m ()
log s msg = P.yield (LogEntry s msg)

runLoggerIO :: MonadIO m => Logger m () -> m ()
runLoggerIO logged = P.runEffect $ P.for logged (liftIO . putStrLn . T.unpack . logEntryPretty)
