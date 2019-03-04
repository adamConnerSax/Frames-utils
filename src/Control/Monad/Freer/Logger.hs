{-# LANGUAGE ConstraintKinds       #-}
{-# LANGUAGE DataKinds             #-}
{-# LANGUAGE FlexibleContexts      #-}
{-# LANGUAGE FlexibleInstances     #-}
{-# LANGUAGE GADTs                 #-}
{-# LANGUAGE LambdaCase            #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings     #-}
{-# LANGUAGE RankNTypes            #-}
{-# LANGUAGE ScopedTypeVariables   #-}
{-# LANGUAGE TypeApplications      #-}
{-# LANGUAGE TypeOperators         #-}
{-# LANGUAGE UndecidableInstances  #-}
{-# OPTIONS_GHC -fwarn-incomplete-patterns #-}
module Control.Monad.Freer.Logger
  (
    LogSeverity (..)
  , LogEntry(..)
  , Logger (..)
  , PrefixLog
  , logAll
  , nonDiagnostic
--  , logMessage
  , log
  , logLE
  , wrapPrefix
  , logToStdoutSimple
  , logPrefixedToStdout
  , logToStdoutLE
  , logToMonadLog
  , logPrefixedToMonadLog
  , logToMonadLogLE
  , PrefixedLogEffs
  , LogWithPrefixes

--  , logToFileAsync
  -- re-exports
  , Eff
  , Member
  ) where


import           Control.Monad.Freer                   (Eff, Member)
import qualified Control.Monad.Freer                   as FR
import qualified Control.Monad.Freer.State             as FR
import qualified Control.Monad.Freer.Writer            as FR
import           Control.Monad.IO.Class                (MonadIO (..))
import qualified Control.Monad.Log                     as ML
import           Control.Monad.Morph                   (MFunctor, MonadTrans,
                                                        generalize, hoist, lift)
import           Control.Monad.State                   (MonadState, State,
                                                        StateT, evalState,
                                                        evalStateT, get, modify)
import           Data.Functor.Identity                 (Identity)
import qualified Data.List                             as List
import           Data.Monoid                           ((<>))
import qualified Data.Text                             as T
import qualified Data.Text.IO                          as T
import qualified Data.Text.Prettyprint.Doc             as PP
import qualified Data.Text.Prettyprint.Doc.Render.Text as PP
import qualified Pipes                                 as P
import           Prelude                               hiding (log)
import qualified System.IO                             as Sys


-- TODO: add a runner to run in MonadLogger
-- http://hackage.haskell.org/package/logging-effect

-- a simple type for logging text with a subset of severities

data LogSeverity = Diagnostic | Info | Warning | Error deriving (Show, Eq, Ord, Enum, Bounded)

logSeverityToSeverity :: LogSeverity -> ML.Severity
logSeverityToSeverity Diagnostic = ML.Debug
logSeverityToSeverity Info       = ML.Informational
logSeverityToSeverity Warning    = ML.Warning
logSeverityToSeverity Error      = ML.Error

data LogEntry = LogEntry { severity :: LogSeverity, message :: T.Text }

logEntryToWithSeverity :: LogEntry -> ML.WithSeverity T.Text
logEntryToWithSeverity (LogEntry s t) = ML.WithSeverity (logSeverityToSeverity s) t

logEntryPretty :: LogEntry -> T.Text
logEntryPretty (LogEntry Diagnostic  d) = "(Diagnostic): " <> d
logEntryPretty (LogEntry Info t)        = "(Info): " <> t
logEntryPretty (LogEntry Warning w)     = "(Warning): " <> w
logEntryPretty (LogEntry Error  e)      = "(Error): " <> e

filterLogEntry :: [LogSeverity] -> LogEntry -> Maybe LogEntry
filterLogEntry ls (LogEntry s m) = if (s `elem` ls) then Just (LogEntry s m) else Nothing

logAll :: [LogSeverity]
logAll = [minBound..maxBound]

nonDiagnostic :: [LogSeverity]
nonDiagnostic = List.tail logAll

type LoggerPrefix = [T.Text]

-- output
data Logger a r where
  Log :: a -> Logger a ()

log :: FR.Member (Logger a) effs => a -> FR.Eff effs ()
log = FR.send . Log

logLE :: FR.Member (Logger LogEntry) effs => LogSeverity -> T.Text -> FR.Eff effs ()
logLE ls lm = log (LogEntry ls lm)

logToStdoutSimple :: MonadIO (FR.Eff effs) => (a -> T.Text) -> FR.Eff (Logger a ': effs) x -> FR.Eff effs x
logToStdoutSimple toText = FR.interpret (\(Log a) -> liftIO $ T.putStrLn $ toText a)


-- Add a prefix system for wrapping logging
data PrefixLog r where
  AddPrefix :: T.Text -> PrefixLog ()
  RemovePrefix :: PrefixLog ()
  GetPrefix :: PrefixLog T.Text

addPrefix :: FR.Member PrefixLog effs => T.Text -> FR.Eff effs ()
addPrefix = FR.send . AddPrefix

removePrefix :: FR.Member PrefixLog effs => FR.Eff effs ()
removePrefix = FR.send RemovePrefix

getPrefix :: FR.Member PrefixLog effs => FR.Eff effs T.Text
getPrefix = FR.send $ GetPrefix

wrapPrefix :: FR.Member PrefixLog effs => T.Text -> FR.Eff effs a -> FR.Eff effs a
wrapPrefix p l = do
  addPrefix p
  res <- l
  removePrefix
  return res

-- interpret LogPrefix in State
prefixInState :: forall effs a. FR.Eff (PrefixLog ': effs) a -> FR.Eff (FR.State [T.Text] ': effs) a
prefixInState = FR.reinterpret $ \case
    AddPrefix t -> FR.modify (\ps -> t : ps)
    RemovePrefix -> FR.modify @[T.Text] tail -- type application required here since tail is polymorphic
    GetPrefix -> (FR.get >>= (return . T.intercalate "." . List.reverse))


logPrefixedToStdout :: MonadIO (FR.Eff effs) => (a -> Maybe T.Text) -> FR.Eff (Logger a ': (PrefixLog ': effs)) x -> FR.Eff effs x
logPrefixedToStdout toTextM = FR.evalState []
                              . prefixInState
                              . FR.interpret (\(Log a) -> case toTextM a of
                                                 Nothing -> return ()
                                                 Just msg -> do
                                                   p <- getPrefix
                                                   FR.raise $ liftIO $ T.putStrLn $ p <> ": " <> msg)

-- just for LogEntry
logToStdoutLE :: MonadIO (FR.Eff effs) => [LogSeverity] -> FR.Eff (Logger LogEntry ': (PrefixLog ': effs)) x -> FR.Eff effs x
logToStdoutLE lss = logPrefixedToStdout (\(LogEntry ls lm) -> if ls `List.elem` lss then Just lm else Nothing)


-- add a prefix to the log message and render
data WithPrefix a = WithPrefix { msgPrefix :: T.Text, discardPrefix :: a }
renderWithPrefix :: (a -> PP.Doc ann) -> WithPrefix a -> PP.Doc ann
renderWithPrefix k (WithPrefix pr a) = PP.pretty pr PP.<+> PP.pretty (": " :: T.Text) PP.<+> PP.align (k a)

logToMonadLog :: ML.MonadLog a (FR.Eff effs) => (a -> Bool) -> FR.Eff (Logger a ': effs) x -> FR.Eff effs x
logToMonadLog filterLog = FR.interpret (\(Log a) -> if filterLog a then ML.logMessage a else return ())

logPrefixedToMonadLog :: ML.MonadLog (WithPrefix a) (FR.Eff effs) => (a -> Bool) -> FR.Eff (Logger a ': (PrefixLog ': effs)) x -> FR.Eff effs x
logPrefixedToMonadLog filterLog = FR.evalState []
                        . prefixInState
                        . FR.interpret (\(Log a) -> case filterLog a of
                                           False -> return ()
                                           True -> do
                                             p <- getPrefix
                                             FR.raise $ ML.logMessage (WithPrefix p a))

logToMonadLogLE :: ML.MonadLog (WithPrefix LogEntry) (FR.Eff effs) => [LogSeverity] -> FR.Eff (Logger LogEntry ': (PrefixLog ': effs)) x -> FR.Eff effs x
logToMonadLogLE lss = logPrefixedToMonadLog (\(LogEntry ls _) -> ls `List.elem` lss)

type PrefixedLogEffs a = '[PrefixLog, Logger a]
type LogWithPrefixes effs = FR.Members (PrefixedLogEffs LogEntry) effs

instance (ML.MonadLog a m, FR.LastMember m effs) => ML.MonadLog a (FR.Eff effs) where
  logMessageFree inj = FR.sendM $ ML.logMessageFree inj



{-
writeLogStdout :: MonadIO (FR.Eff effs) => FR.Eff (LogWriter ': effs) a -> FR.Eff effs a
writeLogStdout = FR.interpret (\(WriteToLog t) -> liftIO $ T.putStrLn t)

writeLogToFileAsync :: MonadIO (FR.Eff effs) => Sys.Handle -> FR.Eff (LogWriter ': effs) a -> FR.Eff effs a
writeLogToFileAsync h = FR.interpret (\(WriteToLog t) -> liftIO $ T.hPutStrLn h t)

logToFileAsync :: MonadIO (FR.Eff effs) => Sys.Handle -> [LogSeverity] -> FR.Eff (Logger ': effs) a -> FR.Eff effs a
logToFileAsync h lss = writeLogToFileAsync h . runLogger lss
-}



