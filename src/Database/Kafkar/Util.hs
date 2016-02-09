
module Database.Kafkar.Util
    ( dropUntilFirstUnder
    , dropUntilLastSuchThat
    , dropUntilLastSuchThatP
    , findLastSuchThat
    ) where

import Data.Maybe
import Pipes

findLastSuchThat :: (a -> Bool) -> [a] -> Maybe a
findLastSuchThat p = listToMaybe . dropUntilLastSuchThat p


dropUntilLastSuchThatP :: Monad m => (a -> Bool) -> Pipe a a m ()
dropUntilLastSuchThatP p = await >>= go0
  where
    go0 x
      | p x = yield x >> cat
      | otherwise = await >>= go1 x

    go1 old new
      | p new = yield old >> yield new >> cat
      | otherwise = await >>= go1 new




-- | Drops the longest prefix which satisfies the predicate, except the
-- last element of that prefix.
dropUntilLastSuchThat :: (a -> Bool) -> [a] -> [a]
dropUntilLastSuchThat p = go0
  where
    go0 [] = []
    go0 (x:xs) | p x = go1 x xs
    go0 (x:xs) = x:xs

    go1 prev [] = prev:[]
    go1 _    (x:xs) | p x = go1 x xs
    go1 prev (x:xs) = prev:x:xs


-- dropUntilFirstUnder compare 1 [2,4,6]  =>  [2,4,6]
-- dropUntilFirstUnder compare 2 [2,4,6]  =>  [2,4,6]
-- dropUntilFirstUnder compare 3 [2,4,6]  =>  [2,4,6]
-- dropUntilFirstUnder compare 4 [2,4,6]  =>  [4,6]
-- dropUntilFirstUnder compare 5 [2,4,6]  =>  [4,6]
-- dropUntilFirstUnder compare 6 [2,4,6]  =>  [6]
-- dropUntilFirstUnder compare 7 [2,4,6]  =>  [6]
dropUntilFirstUnder :: (a -> b -> Ordering) -> a -> [b] -> [b]
dropUntilFirstUnder comp target = dropUntilLastSuchThat lte
  where lte x = case comp target x of
                   LT -> False
                   EQ -> True
                   GT -> True



-- dropUntilFirstUnder comp target = go0
--   where
--     go0 (x:xs) = case comp target x of
--         LT -> x:xs         -- The target precedes the whole input list
--         EQ -> x:xs         -- The target is the head of the input list
--         GT -> go1 x xs     -- Not there yet; keep going.
--     go0 [] = []

--     go1 prev (x:xs) = case comp target x of
--         LT -> prev:x:xs    -- We've overshot the target; return.
--         EQ -> x:xs         -- We're at the target; return.
--         GT -> go1 x xs     -- We're not there yet; keep going.
--     go1 prev [] = prev:[]  -- We're at the end but still didn't see the target.
