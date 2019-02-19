{-# LANGUAGE FlexibleContexts  #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes        #-}
{-# LANGUAGE GADTs             #-}
{-# LANGUAGE LambdaCase        #-}
{-# OPTIONS_GHC -fwarn-incomplete-patterns #-}
module System.PipesLogger where

import           Control.Monad.IO.Class (MonadIO (..))
import           Control.Monad.State    (MonadState, State, evalState, StateT, evalStateT, get,
                                         modify)
import           Control.Monad.Morph (lift, hoist, generalize, MonadTrans, MFunctor)
import           Data.Functor.Identity (Identity)
import qualified Data.List              as List
import           Data.Monoid            ((<>))
import qualified Data.Text              as T
import qualified Pipes                  as P
import qualified Control.Monad.Freer    as FR
import qualified Control.Monad.Freer.State   as FR
import qualified Control.Monad.Freer.Writer   as FR


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

type LoggerPrefix = [T.Text]

-- NB: This will cause a problem if the underlying monad has State in it.  So this works for this project but not in general.  And I had
-- to fiddle with the random source (put it in an IORef rather than State) to get it to work.  Would likely be better re-designed via
-- a lensed state so that all we need is state with the LoggerPrefix present.

-- Also, just Haskell confusing me:  how does this log in real time before things are finished now that it needs the state?  Pipes are confusing magic.


data Logger r where
  LogText :: LogSeverity -> T.Text -> Logger ()
  AddPrefix :: T.Text -> Logger ()
  RemovePrefix :: Logger ()

log :: FR.Member Logger effs => LogSeverity -> T.Text -> FR.Eff effs ()
log ls t = FR.send $ LogText ls t

addPrefix :: FR.Member Logger effs => T.Text -> FR.Eff effs ()
addPrefix = FR.send . AddPrefix

removePrefix :: FR.Member Logger effs => FR.Eff effs ()
removePrefix = FR.send RemovePrefix

-- interpret in State (for prefixes) and Writer (for output)
runLogger :: FR.Eff '[Logger : effs] a -> FR.Eff '[FR.Writer [T.Text] : effs a]
runLogger l = FR.runState [] (FR.reinterpret2 go l) where
  go :: forall a effs. Logger a -> FR.Eff '[FR.State [T.Text] : [FR.Writer [T.Text] : effs]] a
  go (LogText ls t) = do
    ps <- FR.get
    let prefixText = T.intercalate "." (List.reverse prefixes)
    FR.tell [prefixText <> ": " <> (logEntryPretty $ LogEntry (ls t))]
  go (AddPrefix t) = FR.modify (\ps -> t : ps)  
  go RemovePrefix = FR.modify tail
{-
type Logger m = P.Producer LogEntry (StateT LoggerPrefix m)

log :: Monad m => LogSeverity -> T.Text -> Logger m ()
log s msg = P.yield (LogEntry s msg)

addPrefix :: Monad m => T.Text -> Logger m ()
addPrefix t = modify (\ps -> t : ps)

removePrefix :: Monad m => Logger m ()
removePrefix = modify tail
-}



wrapPrefix :: FR.Member Logger effs => T.Text -> FR.Eff effs a -> FR.Effs effs a
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

liftAction :: Monad m => m a -> Logger m a
liftAction = lift . lift

liftPureAction :: (MonadTrans t, Monad (t IO), MFunctor t) => t Identity a -> Logger (t IO) a
liftPureAction a = liftAction $ hoist generalize a

liftFunction :: forall m b. Monad m => (forall a. m a -> m a) -> Logger m b -> Logger m b
liftFunction f = hoist (hoist f)


