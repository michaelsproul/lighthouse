/// Trait for types that we can compute a maximum cover for.
///
/// Terminology:
/// * `item`: something that implements this trait
/// * `element`: something contained in set, and covered by the covering set of an item
/// * `object`: something extracted from an item in order to comprise a solution
/// See: https://en.wikipedia.org/wiki/Maximum_coverage_problem
pub trait MaxCover {
    /// The result type, of which we would eventually like a collection of maximal quality.
    type Object;
    /// The type used to represent sets.
    type Set: Clone;

    /// Extract an object for inclusion in a solution.
    fn object(&self) -> Self::Object;

    /// Get the set of elements covered.
    fn covering_set(&self) -> &Self::Set;
    /// Update the set of items covered, for the inclusion of some object in the solution.
    fn update_covering_set(&mut self, max_obj: &Self::Object, max_set: &Self::Set);
    /// The quality of this item's covering set, usually its cardinality.
    fn score(&self) -> usize;
}

/// Helper struct to track which items of the input are still available for inclusion.
/// Saves removing elements from the work vector.
struct MaxCoverItem<T> {
    item: T,
    available: bool,
}

impl<T> MaxCoverItem<T> {
    fn new(item: T) -> Self {
        MaxCoverItem {
            item,
            available: true,
        }
    }
}

/// Compute an approximate maximum cover using a greedy algorithm.
///
/// * Time complexity: `O(limit * items_iter.len())`
/// * Space complexity: `O(item_iter.len())`
pub fn maximum_cover<'a, I, T>(items_iter: I, limit: usize) -> Vec<T::Object>
where
    I: IntoIterator<Item = T>,
    T: MaxCover,
{
    // Construct an initial vec of all items, marked available.
    let mut all_items: Vec<_> = items_iter
        .into_iter()
        .map(MaxCoverItem::new)
        .filter(|x| x.item.score() != 0)
        .collect();

    let mut result = vec![];

    for _ in 0..limit {
        // Select the item with the maximum score.
        let (best_item, best_cover) = match all_items
            .iter_mut()
            .filter(|x| x.available && x.item.score() != 0)
            .max_by_key(|x| x.item.score())
        {
            Some(x) => {
                x.available = false;
                (x.item.object(), x.item.covering_set().clone())
            }
            None => return result,
        };

        // Update the covering sets of the other items, for the inclusion of the selected item.
        // Items covered by the selected item can't be re-covered.
        all_items
            .iter_mut()
            .filter(|x| x.available && x.item.score() != 0)
            .for_each(|x| x.item.update_covering_set(&best_item, &best_cover));

        result.push(best_item);
    }

    result
}
