use serde_json::{Map, Value};

/// A mutable, kind-0 profile metadata document.
///
/// Wraps a JSON object (always a [`Value::Object`]) holding the standard
/// nostr profile fields (`name`, `about`, `picture`, ...). Provides typed
/// accessors plus mutation helpers used when editing a profile before it is
/// serialized back into a kind-0 note.
#[derive(Debug, Clone)]
pub struct ProfileState(Value);

impl Default for ProfileState {
    fn default() -> Self {
        ProfileState::new(Map::default())
    }
}

impl ProfileState {
    /// Create a [`ProfileState`] from an existing JSON object map.
    pub fn new(value: Map<String, Value>) -> Self {
        Self(Value::Object(value))
    }

    /// Get a string-valued field by name, if present and a string.
    pub fn get_str(&self, name: &str) -> Option<&str> {
        self.0.get(name).and_then(|v| v.as_str())
    }

    /// Mutable access to the underlying object map.
    pub fn values_mut(&mut self) -> &mut Map<String, Value> {
        self.0.as_object_mut().unwrap()
    }

    /// Insert or overwrite an existing value with a string
    pub fn str_mut(&mut self, name: &str) -> &mut String {
        let val = self
            .values_mut()
            .entry(name)
            .or_insert(Value::String("".to_string()));

        // if its not a string, make it one. this will overrwrite
        // the old value, so be careful
        if !val.is_string() {
            *val = Value::String("".to_string());
        }

        match val {
            Value::String(s) => s,
            // SAFETY: we replace it above, so its impossible to be something
            // other than a string
            _ => panic!("impossible"),
        }
    }

    /// The underlying JSON value.
    pub fn value(&self) -> &Value {
        &self.0
    }

    /// Serialize the profile object to a JSON string.
    pub fn to_json(&self) -> String {
        // SAFETY: serializing a value should be irrefutable
        serde_json::to_string(self.value()).unwrap()
    }

    #[inline]
    pub fn name(&self) -> Option<&str> {
        self.get_str("name")
    }

    #[inline]
    pub fn banner(&self) -> Option<&str> {
        self.get_str("name")
    }

    #[inline]
    pub fn display_name(&self) -> Option<&str> {
        self.get_str("display_name")
    }

    #[inline]
    pub fn lud06(&self) -> Option<&str> {
        self.get_str("lud06")
    }

    #[inline]
    pub fn nip05(&self) -> Option<&str> {
        self.get_str("nip05")
    }

    #[inline]
    pub fn lud16(&self) -> Option<&str> {
        self.get_str("lud16")
    }

    #[inline]
    pub fn about(&self) -> Option<&str> {
        self.get_str("about")
    }

    #[inline]
    pub fn picture(&self) -> Option<&str> {
        self.get_str("picture")
    }

    #[inline]
    pub fn website(&self) -> Option<&str> {
        self.get_str("website")
    }

    /// Parse a [`ProfileState`] from the JSON contents of a kind-0 note,
    /// falling back to an empty object if the contents don't parse to an object.
    pub fn from_note_contents(contents: &str) -> Self {
        let json = serde_json::from_str(contents);
        let data = if let Ok(Value::Object(data)) = json {
            data
        } else {
            Map::new()
        };

        Self::new(data)
    }
}
