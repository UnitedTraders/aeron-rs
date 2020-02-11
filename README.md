# Aeron client written in Rust
## Setup git hooks

If you use POSIX-compliant OS (or at least has `/bin/sh`), you can use
predefined git [pre-push](.pre-push.sh) hook to ensure coding standards are met.

```
ln -s ../../.pre-push.sh .git/hooks/pre-push
```

Also, if you use IDE from IntellijiIdea family with Rust plugin, you can enable autoformatting:

`Settings -> Languages and Frameworks -> Rust -> Rustfmt -> Run rustfmt on Save`