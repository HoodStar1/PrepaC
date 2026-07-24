# FAQ

## Which systems are supported?

Docker on Linux AMD64, direct Linux x86-64, and direct Windows x64. Direct installs use Python 3.13 or 3.14. macOS and ARM64 are not supported.

## Is rsync required?

No. Docker and Linux installs can use it as a copy accelerator. Windows uses the built-in copy path and does not require rsync.

## Where is configuration stored?

`PREPAC_CONFIG_DIR` wins when set. Otherwise the launcher uses `/config` in Docker, `$XDG_CONFIG_HOME/prepac` or `~/.config/prepac` on Linux, and `%LOCALAPPDATA%\PrepaC` on Windows.

## Why does Linux use only one Gunicorn worker?

Background-job ownership and active-job tracking are process-local. One worker with multiple threads avoids two processes believing they own the same in-memory state. Tune threads first.

## What does outcome_unknown mean?

PrepaC lost reliable confirmation while a job was finalizing or uploading, so it cannot safely claim success or failure. Check and reconcile the local or remote destination before resubmitting. Prepare, Packing, and Posting keep duplicate prevention active until an administrator explicitly acknowledges the verified destination; that acknowledgement enables a new manual submission but does not retry automatically. A forced Share retry can create a duplicate if the first upload actually succeeded.

## Is the Freeimage API key a TMDB key?

No. It is only for **freeimage.host**.

## What happens if I leave the end tag unchanged?

PrepaC uses **PrepaC** as the default release tag.

## Can Share import files with different names?

Yes. PrepaC can pair mass imports by filename and by template content.

## Do I need to re-enter existing provider credentials after upgrading?

No. Existing Provider 1 and Provider 2 setups carry forward into the provider builder.

## What does “Prioritize jobs up to (GB)” mean?

For providers after Provider 1, a non-zero value prefers smaller eligible jobs on that provider. Lower non-zero limits are considered first. A value of **0** keeps the provider in the default availability pool with Provider 1.
