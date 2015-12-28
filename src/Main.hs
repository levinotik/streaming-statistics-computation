{-# LANGUAGE OverloadedStrings #-}
module Main where

import Data.Conduit
import qualified Data.Conduit.List as CL
import qualified Data.Conduit.Binary as CB
import Data.CSV.Conduit
import Data.Text (Text, pack)
import qualified Data.Text as T
import Data.CSV.Conduit.Conversion
import Control.Monad
import Data.Vector (fromList)
import Control.Error.Util (hush)
import Control.Monad.Trans.Resource (ResourceT)
import Model
import Control.DeepSeq
import System.Environment (getArgs)
import System.Exit (die)
import Data.Maybe
import Data.Monoid ((<>))
import qualified Data.Text.IO as TIO

main :: IO ()
main = do  
  files <- getArgs
  when (null files) (die usage)
  stats <- mapM (\csv -> runResourceT $ 
            rowsToMetrics csv $$ computeStats) files 
  mapM_ (TIO.putStr . showStats) stats  
  
rowsToMetrics ::  FilePath -> Source (ResourceT IO) Metric
rowsToMetrics path = CB.sourceFile path $=
                     intoCSV defCSVSettings $=
                     (CL.drop 1 >> CL.map id) $= 
                     CL.mapMaybe (hush . runParser . parseRecord . fromList)

computeStats :: Consumer Metric (ResourceT IO) Stats 
computeStats = CL.fold foldStats defStats  
  
usage :: String
usage = "Usage: ./wagon [path(s) to .csv files]"  

showStats :: Stats -> Text 
showStats Stats{..} = 
  let texts = [ "sessionId columns: " <> showText (sessionIdColumn statsColumnCount)
              , "page columns: " <> showText (pageColumn statsColumnCount)
              , "latency columns: " <> showText (latencyColumn statsColumnCount)
              , "timeOnPage columns: " <> showText (timeOnPageColumn statsColumnCount)
              , "null sessionId columns: " <> showText  (sessionIdColumn statsColumnNullCount)
              , "null page columns: " <> showText (pageColumn statsColumnNullCount)
              , "null latency columns: " <> showText (latencyColumn statsColumnNullCount)
              , "null timeOnPage columns: " <> showText (timeOnPageColumn statsColumnNullCount)
              , "latency aggregate " <> showText statsLatencyAgg
              , "timeOnPage aggregate " <> showText statsTimeOnPageAgg
              , "sessionId text stats " <> showText sessionIdTextStats
              , "page text stats " <> showText pageTextStats
              ]
  in T.unlines texts

showText :: Show a => a -> Text
showText = pack . show

foldStats :: Stats -> Metric -> Stats
foldStats (Stats s1 s2 s3 s4 s5 s6) metric = 
  let colCount        = trackColCount s1
      nullCount       = trackNulls s2 metric
      latencyCount    =  (latencyColumn colCount - latencyColumn nullCount)
      timeOnPageCount =  (timeOnPageColumn colCount - timeOnPageColumn nullCount)
      latencyAgg      = trackLatencyAgg s3 metric latencyCount
      timeOnPageAgg   = trackTimeOnPageAgg s4 metric timeOnPageCount
      sessionIdTextStats = case metricSessionId metric of
        SessionId text -> trackText s5 text (sessionIdColumn colCount - sessionIdColumn nullCount)
        SNull          -> s5
      newPageTextStats = case metricPage metric of
        Page text -> trackText s6 text (pageColumn colCount - pageColumn nullCount)
        PNull     -> s6
  in force (Stats colCount nullCount latencyAgg timeOnPageAgg sessionIdTextStats newPageTextStats)
  where 
    trackNulls (StatsColumnCount c1 c2 c3 c4) (Metric m1 m2 m3 m4) = 
      StatsColumnCount (trackNull m1 c1) (trackNull m2 c2) (trackNull m3 c3) (trackNull m4 c4)
    trackNull :: Nullable m => m -> Double -> Double 
    trackNull m c = if isNull m then c + 1 else c   
    trackColCount (StatsColumnCount c1 c2 c3 c4) = StatsColumnCount (c1 + 1) (c2 + 1) (c3 + 1) (c4 +1)
    trackLatencyAgg (NumberAggregate (Minimum min') (Maximum max') (Average avg') (RunningTotal rTotal))
      (Metric _ _ (Latency l) _) count = 
        let newMin =  comparing Minimum (fromIntegral l) min min' 
            newMax =  comparing Maximum (fromIntegral l) max max'
            newRtotal = maybe (RunningTotal $ Just $ fromIntegral l) (RunningTotal . Just . (+ fromIntegral  l)) rTotal
            newAvg =  case avg' of 
              Nothing -> Average (Just $ fromIntegral l)
              _       -> Average (Just (fromMaybe 0 (unRunningTotal newRtotal) / count))
        in NumberAggregate newMin newMax newAvg newRtotal   
    trackLatencyAgg agg _ _ = agg
    trackTimeOnPageAgg (NumberAggregate (Minimum min') (Maximum max') (Average avg') (RunningTotal rTotal))
       (Metric _ _ _ (TimeOnPage t)) count = 
         let newMin =  comparing Minimum t min min'
             newMax =  comparing Maximum t max max'
             newRtotal = maybe (RunningTotal $ Just t) (RunningTotal . Just . (+ t)) rTotal
             newAvg =  case avg' of 
               Nothing -> Average (Just t)
               _       -> Average (Just (fromMaybe 0 (unRunningTotal newRtotal) / count))
         in NumberAggregate newMin newMax newAvg newRtotal
    trackTimeOnPageAgg agg _ _ = agg
    comparing m value comparison = maybe (m $ Just value) (m . Just . comparison value)
    trackText :: TextStats -> Text -> Double -> TextStats
    trackText (TextStats (ShortestCount shortest) (LongestCount longest) (Average average) (RunningTotal rTotal)) text colCount =
      let newShortest = maybe (ShortestCount (Just (TextCount text 1))) (`calcShortest` text) shortest 
          newLongest = maybe (LongestCount (Just (TextCount text 1))) (`calcLongest` text) longest 
          newRtotal = maybe (RunningTotal $ Just $ fromIntegral $ T.length text) (RunningTotal . Just . (+ (fromIntegral $ T.length text))) rTotal
          newAvg =  case average of 
            Nothing -> Average (Just $ fromIntegral $ T.length text)
            _       -> Average (Just (fromMaybe 0 (unRunningTotal newRtotal) / colCount))
      in TextStats newShortest newLongest newAvg newRtotal   
      
calcShortest :: TextCount -> Text -> ShortestCount
calcShortest t text = if textCountText t == text 
                      then ShortestCount (Just (TextCount text (textCountCount t + 1))) 
                      else case T.length (textCountText t) `compare` T.length text of
                             LT -> ShortestCount $ Just t
                             GT -> ShortestCount (Just (TextCount text 1))
                             EQ -> let min' = textCountText t `min` text 
                                       count' = case textCountText t `compare` text of 
                                                  LT -> textCountCount t
                                                  GT -> 1
                                                  EQ -> textCountCount t + 1
                                   in ShortestCount (Just (TextCount min' count'))  

calcLongest :: TextCount -> Text -> LongestCount
calcLongest t text = if textCountText t == text 
                     then LongestCount (Just (TextCount text (textCountCount t + 1))) 
                     else case T.length (textCountText t) `compare` T.length text of
                           GT -> LongestCount $ Just t
                           LT -> LongestCount (Just (TextCount text 1))
                           EQ -> let max' = textCountText t `max` text 
                                     count' = case textCountText t `compare` text of 
                                                GT -> textCountCount t
                                                LT -> 1
                                                EQ -> textCountCount t + 1
                                 in LongestCount (Just (TextCount max' count'))      