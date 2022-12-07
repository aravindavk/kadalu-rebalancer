# Kadalu Storage Rebalance

Or GlusterFS.

## Build

```
shards build
```

## Usage

After running `fix-layout` (Will cover this later)

```
./bin/kadalu-rebalancer <brick-root> <mount-path>
```

Example:

```
./bin/kadalu-rebalancer /bricks/vol1/b1 /mnt/vol1
```


