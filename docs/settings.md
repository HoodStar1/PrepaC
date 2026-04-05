# Settings

## Important notes

- The default end tag is **PrepaC**
- Plex is optional but improves posters and watched-state cleanup
- The Freeimage key is only for **freeimage.host** thumbnail upload during packing
- It is **not** a TMDB key

## Posting provider size behavior

### Provider 2 max job size when provider 1 is busy (GB)

This setting controls how job sizes are distributed between Provider 1 and Provider 2 during posting.

- When set above **0**:
  - Provider 1 prioritizes larger jobs first
  - Smaller jobs can be handled by Provider 2 while larger jobs are still packing or waiting to post
- When set to **0**:
  - The behavior is disabled
  - Both providers can process any size job, with Provider 1 taking priority as usual
