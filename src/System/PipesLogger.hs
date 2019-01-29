{-# LANGUAGE FlexibleContexts  #-}
{-# LANGUAGE OverloadedStrings #-}
{-# OPTIONS_GHC -fwarn-incomplete-patterns #-}
module System.PipesLogger where

import           Control.Monad.IO.Class (MonadIO (..))
import           Control.Monad.State    (MonadState, StateT, evalStateT, get,
                                         modify)
import Control.Monad.Trans (lift)                 
import qualified Data.List              as List
import           Data.Monoid            ((<>))
import qualified Data.Text              as T
import qualified Pipes                  as P

data LogSeverity = Diagnostic | Info | Warning | Error deriving (Show, Eq, Ord, Enum, Bounded)
data LogEntry = LogEntry { severity :: LogSeverity, message :: T.Text }

logEntryPretty :: LogEntry -> T.Text
logEntryPretty (LogEntry Diagnostic  d)  = "(Diagnostic): " <> d
logEntryPretty (LogEntry Info t)    = "(Info): " <> t
logEntryPretty (LogEntry Warning w) = "(Warning): " <> w
logEntryPretty (LogEntry Error  e)  = "(Error): " <> e


filterLogEntry :: [LogSeverity] -> LogEntry -> Maybe LogEntry
filterLogEntry ls (LogEntry s m) = if (s `elem` ls) then Just (LogEntry s m) else Nothing

logAll :: [LogSeverity]
logAll = [minBound..maxBound]

nonDiagnostic :: [LogSeverity]
nonDiagnostic = List.tail logAll 

-- NB: This will cause a problem if the underlying monad has State in it.  So this works for this project but not in general.  And I had
-- to fiddle with the random source (put it in an IORef rather than State) to get it to work.  Would likely be better re-designed via
-- a lensed state so that all we need is state with the LoggerPrefix present.

-- Also, just Haskell confusing me:  how does this log in real time before things are finished now that it needs the state?  Pipes are confusing magic.


type LoggerPrefix = [T.Text]
type Logger m = P.Producer LogEntry (StateT LoggerPrefix m)

log :: Monad m => LogSeverity -> T.Text -> Logger m ()
log s msg = P.yield (LogEntry s msg)

addPrefix :: Monad m => T.Text -> Logger m ()
addPrefix t = modify (\ps -> t : ps)

removePrefix :: Monad m => Logger m ()
removePrefix = modify tail

wrapPrefix :: Monad m => T.Text -> Logger m a -> Logger m a
wrapPrefix p l = do
  addPrefix p
  res <- l
  removePrefix
  return res
  
doLogOutput :: (MonadState LoggerPrefix m, MonadIO m) => [LogSeverity] -> LogEntry -> m ()
doLogOutput ls le = case filterLogEntry ls le of
  Nothing -> return ()
  Just x -> do
    ps <- get
    liftIO $ putStrLn $ T.unpack $ {- T.replicate (List.length ps) " " <> -} T.intercalate "." (List.reverse ps) <> " " <> logEntryPretty x

runLogger :: MonadIO m => [LogSeverity] -> Logger m () -> StateT LoggerPrefix m ()
runLogger ls logged = P.runEffect $ P.for logged (doLogOutput ls)

runLoggerIO :: MonadIO m => [LogSeverity] -> Logger m () -> m ()
runLoggerIO ls logged = flip evalStateT [] $ runLogger ls logged

liftLog :: Monad m => m a -> Logger m a
liftLog = lift . lift



