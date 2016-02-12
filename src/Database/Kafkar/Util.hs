
module Database.Kafkar.Util
    ( dropUntilLast
    , findLastInit
    ) where

import qualified Data.Vector.Generic as V

-- | Takes the longest prefix which satisfies the predicate and returns its
-- final element. Returns Nothing if there are no elements at the head of
-- the vector which satisfy the predicate.
findLastInit :: (V.Vector v a) => (a -> Bool) -> v a -> Maybe a
findLastInit p vec =
    let (xs, _) = V.span p vec
    in if V.null xs
         then Nothing
         else Just (V.last xs)

-- | Drops the longest prefix which satisfies the predicate, except the
-- last element of that prefix.
dropUntilLast :: (V.Vector v a) => (a -> Bool) -> v a -> v a
dropUntilLast p vec =
    let (xs, ys) = V.span p vec
    in if V.null xs
         then ys
         else V.last xs `V.cons` ys


-- dropUntilFirstUnder compare 1 [2,4,6]  =>  [2,4,6]
-- dropUntilFirstUnder compare 2 [2,4,6]  =>  [2,4,6]
-- dropUntilFirstUnder compare 3 [2,4,6]  =>  [2,4,6]
-- dropUntilFirstUnder compare 4 [2,4,6]  =>  [4,6]
-- dropUntilFirstUnder compare 5 [2,4,6]  =>  [4,6]
-- dropUntilFirstUnder compare 6 [2,4,6]  =>  [6]
-- dropUntilFirstUnder compare 7 [2,4,6]  =>  [6]
