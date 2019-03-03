{-# LANGUAGE FlexibleContexts  #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes        #-}
{-# LANGUAGE GADTs             #-}
{-# LANGUAGE LambdaCase        #-}
{-# LANGUAGE DataKinds         #-}
{-# LANGUAGE TypeOperators     #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}
{-# OPTIONS_GHC -fwarn-incomplete-patterns #-}
module Control.Monad.Freer.Logger
  (
    LogSeverity (..)
  , Logger(..)
  , logAll
  , nonDiagnostic
  , logMessage
  , log
  , wrapPrefix
  , logToStdout
  , logToFileAsync
  -- re-exports
  , Eff
  , Member
  ) where


import           Prelude                    hiding (log)
import           Control.Monad.IO.Class     (MonadIO (..))
import           Control.Monad.State        (MonadState, State, evalState, StateT, evalStateT, get,
                                            modify)
import           Control.Monad.Morph        (lift, hoist, generalize, MonadTrans, MFunctor)
import           Data.Functor.Identity      (Identity)
import qualified Data.List                  as List
import           Data.Monoid                ((<>))
import qualified Data.Text                  as T
import qualified Data.Text.IO               as T
import qualified Pipes                      as P
import qualified Control.Monad.Freer        as FR
import           Control.Monad.Freer        (Eff, Member)
import qualified Control.Monad.Freer.State  as FR
import qualified Control.Monad.Freer.Writer as FR
import qualified Control.Monad.Log          as ML
import qualified System.IO                  as Sys

-- TODO: add a runner to run in MonadLogger
-- http://hackage.haskell.org/package/logging-effect

data LogSeverity = Diagnostic | Info | Warning | Error deriving (Show, Eq, Ord, Enum, Bounded)

logSeverityToSeverity :: LogSeverity -> ML.Severity
logSeverityToSeverity Diagnostic = ML.Debug
logSeverityToSeverity Info = ML.Informational
logSeverityToSeverity Warning = ML.Warning
logSeverityToSeverity Error = ML.Error

data LogEntry = LogEntry { severity :: LogSeverity, message :: T.Text }

logEntryToWithSeverity :: LogEntry -> ML.WithSeverity Text
logEntryToWithSeverity (LogEntry s t) = ML.WithSeverity (logSeverityToMLSeverity s) m

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

type LoggerPrefix = [T.Text]

-- output
data LogWriter a r where
  WriteToLog :: a -> LogWriter ()

writeToLog :: FR.Member LogWriter effs => a -> FR.Eff effs ()
writeToLog = FR.send . WriteToLog 

-- NB: First one is more complex than it has to be but that allows us to defer unwrapping the message until it's logged
data Logger r where
  LogMessage :: (a -> LogSeverity) -> (a -> T.Text) -> a -> Logger ()
  AddPrefix :: T.Text -> Logger ()
  RemovePrefix :: Logger ()

logMessage :: FR.Member Logger effs => (a -> LogSeverity) -> (a -> T.Text) -> a -> FR.Eff effs ()
logMessage toS toT lm = FR.send $ LogMessage toS toT lm

log :: FR.Member Logger effs => LogSeverity -> T.Text -> FR.Eff effs ()
log ls lm = logMessage severity message (LogEntry ls lm)

addPrefix :: FR.Member Logger effs => T.Text -> FR.Eff effs ()
addPrefix = FR.send . AddPrefix

removePrefix :: FR.Member Logger effs => FR.Eff effs ()
removePrefix = FR.send RemovePrefix

wrapPrefix :: FR.Member Logger effs => T.Text -> FR.Eff effs a -> FR.Eff effs a
wrapPrefix p l = do
  addPrefix p
  res <- l
  removePrefix
  return res
  
-- interpret in State (for prefixes) and WriteToLog
runLogger :: forall effs a. [LogSeverity] -> FR.Eff (Logger ': effs) a -> FR.Eff (LogWriter ': effs) a
runLogger lss = FR.evalState [] . loggerToStateLogWriter where
  loggerToStateLogWriter :: forall x. FR.Eff (Logger ': effs) x -> FR.Eff (FR.State [T.Text] ': (LogWriter ': effs)) x
  loggerToStateLogWriter = FR.reinterpret2 $ \case
    LogMessage toS toT lm -> do
      let severity = toS lm
      case severity `elem` lss of
        False -> return ()
        True -> do
          ps <- FR.get
          let prefixText = T.intercalate "." (List.reverse ps)
          writeToLog $ prefixText <> ": " <> (logEntryPretty (LogEntry severity (toT lm))) -- we only convert to Text if we have to
    AddPrefix t -> FR.modify (\ps -> t : ps)  
    RemovePrefix -> FR.modify @[T.Text] tail -- type application required here since tail is polymorphic

writeLogStdout :: MonadIO (FR.Eff effs) => FR.Eff (LogWriter ': effs) a -> FR.Eff effs a
writeLogStdout = FR.interpret (\(WriteToLog t) -> liftIO $ T.putStrLn t)

writeLogToFileAsync :: MonadIO (FR.Eff effs) => Sys.Handle -> FR.Eff (LogWriter ': effs) a -> FR.Eff effs a
writeLogToFileAsync h = FR.interpret (\(WriteToLog t) -> liftIO $ T.hPutStrLn h t)

logToFileAsync :: MonadIO (FR.Eff effs) => Sys.Handle -> [LogSeverity] -> FR.Eff (Logger ': effs) a -> FR.Eff effs a
logToFileAsync h lss = writeLogToFileAsync h . runLogger lss 

logToStdout :: MonadIO (FR.Eff effs) => [LogSeverity] -> FR.Eff (Logger ': effs) a -> FR.Eff effs a
logToStdout lss = writeLogStdout . runLogger lss


  

