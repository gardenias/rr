use super::model::Page;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Default, Deserialize)]
pub(crate) struct PageQuery {
    pub cursor: Option<String>,
    #[serde(default, deserialize_with = "deserialize_page_size")]
    pub page_size: Option<usize>,
}

fn deserialize_page_size<'de, D>(deserializer: D) -> Result<Option<usize>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    Option::<String>::deserialize(deserializer)?
        .map(|value| value.parse().map_err(serde::de::Error::custom))
        .transpose()
}

#[derive(Serialize, Deserialize)]
struct Cursor {
    offset: usize,
}

pub(crate) fn paginate<T: Clone>(items: &[T], query: PageQuery) -> Result<Page<T>, String> {
    let page_size = query.page_size.unwrap_or(50);
    if !(1..=200).contains(&page_size) {
        return Err("page_size must be between 1 and 200".to_string());
    }
    let offset = match query.cursor {
        Some(cursor) => {
            let bytes = URL_SAFE_NO_PAD
                .decode(cursor)
                .map_err(|_| "invalid pagination cursor".to_string())?;
            let cursor: Cursor = serde_json::from_slice(&bytes)
                .map_err(|_| "invalid pagination cursor".to_string())?;
            if cursor.offset > items.len() {
                return Err("pagination cursor is outside the result set".to_string());
            }
            cursor.offset
        }
        None => 0,
    };
    let end = offset.saturating_add(page_size).min(items.len());
    let next_cursor = (end < items.len()).then(|| {
        URL_SAFE_NO_PAD.encode(serde_json::to_vec(&Cursor { offset: end }).expect("cursor JSON"))
    });
    Ok(Page {
        items: items[offset..end].to_vec(),
        next_cursor,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cursor_pages_are_bounded_and_stable() {
        let items = (0..205).collect::<Vec<_>>();
        let first = paginate(&items, PageQuery::default()).unwrap();
        assert_eq!(first.items.len(), 50);
        let cursor = first.next_cursor.unwrap();
        let second = paginate(
            &items,
            PageQuery {
                cursor: Some(cursor),
                page_size: Some(200),
            },
        )
        .unwrap();
        assert_eq!(second.items.len(), 155);
        assert_eq!(second.items[0], 50);
        assert!(second.next_cursor.is_none());
    }

    #[test]
    fn invalid_cursor_and_oversized_pages_are_rejected() {
        let items = vec![1, 2, 3];
        assert!(paginate(
            &items,
            PageQuery {
                cursor: Some("bad".to_string()),
                page_size: None
            }
        )
        .is_err());
        assert!(paginate(
            &items,
            PageQuery {
                cursor: None,
                page_size: Some(201)
            }
        )
        .is_err());
        assert!(paginate(
            &items,
            PageQuery {
                cursor: None,
                page_size: Some(0)
            }
        )
        .is_err());
    }
}
