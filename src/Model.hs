{-# LANGUAGE OverloadedStrings #-}

module Model where 
  
import Data.Text (Text, pack)
import Data.CSV.Conduit.Conversion
import Data.Text.Encoding (decodeUtf8)
import qualified Data.ByteString.Char8 as B8
import qualified Data.ByteString as B
import qualified Data.ByteString.Lex.Fractional as LF
import Data.Vector (fromList)
import Data.Map (Map)
import qualified Data.Map.Strict as Map
import Control.Monad.Identity
import Control.DeepSeq
import GHC.Generics (Generic)


data Metric = Metric 
 { metricSessionId  :: !SessionId
 , metricPage       :: !Page 
 , metricLatency    :: !Latency 
 , metricTimeOnPage :: !TimeOnPage
 } deriving Show
 
newtype Minimum = Minimum { unMiminum :: Maybe Double } deriving Generic
newtype Maximum = Maximum { unMaximum :: Maybe Double } deriving Generic
newtype Average = Average { unAverage :: Maybe Double } deriving Generic
newtype RunningTotal = RunningTotal { unRunningTotal :: Maybe Double } deriving (Show, Generic)
newtype ShortestCount = ShortestCount { unShortest :: Maybe TextCount } deriving (Show, Generic)
newtype LongestCount =  LongestCount { unLongest :: Maybe TextCount } deriving (Show, Generic)

data TextCount = TextCount 
 { textCountText :: Text
 , textCountCount :: Double 
 } deriving Generic
 
instance Show TextCount where 
  show (TextCount text count) = "Text value: " ++ show text ++ " occurences: " ++ show count
  
data Stats = Stats 
  { statsColumnCount     :: StatsColumnCount 
  , statsColumnNullCount :: StatsColumnCount
  , statsLatencyAgg      :: NumberAggregate
  , statsTimeOnPageAgg   :: NumberAggregate
  , sessionIdTextStats   :: TextStats
  , pageTextStats        :: TextStats
  } deriving (Show , Generic)
  
data StatsColumnCount = StatsColumnCount 
  { sessionIdColumn :: !Double
  , pageColumn :: !Double
  , latencyColumn :: !Double
  , timeOnPageColumn :: !Double
  } deriving (Show, Generic)

data TextStats = TextStats ShortestCount LongestCount Average RunningTotal deriving Generic

instance Show TextStats where 
  show (TextStats (ShortestCount shortest) (LongestCount longest) (Average average) (RunningTotal rTotal)) = 
    "Count (shortest) : " ++ maybe "n/a" show shortest ++ " " ++ "Count (longest): " ++ maybe "n/a" show longest ++ " " ++ "Average length: " ++ maybe "n/a" show average

 
data SessionId = SessionId {-# UNPACK #-} !Text | SNull deriving Show

data Page = Page {-# UNPACK #-} !Text | PNull deriving Show

data Latency = Latency {-# UNPACK #-} !Int | LNull deriving Show

data TimeOnPage = TimeOnPage {-# UNPACK #-} !Double | TNull deriving Show

class Nullable r where 
  isNull :: r -> Bool
  
instance Nullable SessionId where 
  isNull (SessionId _) = False
  isNull _ = True

instance Nullable Page where 
  isNull (Page _) = False
  isNull _ = True

instance Nullable Latency where 
  isNull (Latency _) = False
  isNull _ = True

instance Nullable TimeOnPage where 
  isNull (TimeOnPage _) = False
  isNull _ = True
                
instance FromRecord Metric where
    parseRecord v
        | length v == 4 = Metric <$>
                          v .! 0 <*>
                          v .! 1 <*>
                          v .! 2 <*>
                          v .! 3
        | otherwise     = mzero
 
instance FromField SessionId where
    parseField s
        | s == ""   = pure SNull
        | otherwise = pure (SessionId $ decodeUtf8 s)

instance FromField Page where
    parseField s
        | s == ""   = pure PNull
        | otherwise = pure (Page $ decodeUtf8 s)

instance FromField Latency where
    parseField s
        | s == ""   = pure LNull
        | otherwise = pure (maybe LNull (Latency . fst) (B8.readInt s))

instance FromField TimeOnPage where
    parseField s
        | s == ""   = pure TNull
        | otherwise = pure (maybe TNull (TimeOnPage . fst) (LF.readDecimal s))
        
defNumberAggregate :: NumberAggregate 
defNumberAggregate = NumberAggregate (Minimum Nothing) (Maximum Nothing) (Average Nothing) (RunningTotal Nothing)

defTextStats :: TextStats
defTextStats = TextStats (ShortestCount Nothing) (LongestCount Nothing) (Average Nothing) (RunningTotal Nothing)

defStats :: Stats 
defStats = Stats { statsColumnCount     = StatsColumnCount 0 0 0 0 
                 , statsColumnNullCount = StatsColumnCount 0 0 0 0 
                 , statsLatencyAgg      = defNumberAggregate 
                 , statsTimeOnPageAgg   = defNumberAggregate
                 , sessionIdTextStats   = defTextStats  
                 , pageTextStats        = defTextStats }
  

instance NFData Minimum
instance NFData Maximum
instance NFData Average
instance NFData Stats
instance NFData StatsColumnCount
instance NFData NumberAggregate
instance NFData TextStats
instance NFData ShortestCount
instance NFData LongestCount
instance NFData TextCount
instance NFData RunningTotal

instance Show Minimum where 
  show = ("Minimum: " ++) . show . unMiminum 

instance Show Maximum where 
  show = ("Maximum: " ++) . show . unMaximum 

instance Show Average where 
  show = ("Average: " ++) . show . unAverage 

data NumberAggregate = NumberAggregate !Minimum !Maximum !Average !RunningTotal deriving Generic 

instance Show NumberAggregate where 
  show (NumberAggregate aggMin aggMax aggAvg aggRTotal) = 
    show aggMin ++ " " ++ show aggMax ++ " " ++ show aggAvg