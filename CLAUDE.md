# Code cleanliness

- You are NOT ALLOWED to add useless code separators like this:

  ```rust
  // ---------------------------------------------------------------------------
  // Some section
  // ---------------------------------------------------------------------------
  ```

  These are considered bad practice and indicate that the code is not
  well-structured. Prefer using functions and modules to organize your code.

  If you feel the need to add such separators, it likely means that your code
  is too long and should be refactored into smaller, more manageable pieces.
