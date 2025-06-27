# tars2squashfs

Convert multiple tar.gz archives into a single SquashFS filesystem with optimized memory usage and minimal inode consumption.

SquashFS is a read-only filesystem that resides in a single file and can be mounted without root privileges via [squashfuse](https://github.com/vasi/squashfuse). This tool is particularly useful for creating efficient, compressed datasets for machine learning workflows in environments where the number of innodes and limited.


## Requirements

- Python ≥ 3.13
- `mksquashfs` (from squashfs-tools package)
- `squashfuse` (for mounting without root privileges)

### Installing Dependencies

**Ubuntu/Debian:**
```bash
sudo apt-get install squashfs-tools squashfuse
```

**macOS (Homebrew):**
```bash
brew install squashfs
```

**Conda:**
```bash
conda install -c conda-forge squashfuse
```

## Installation

```bash
pip install git+https://github.com/LennartKeller/tars2squashfs.git
```

## Usage

### Basic Usage

```bash
# Convert all tar.gz files in a directory to SquashFS
tars2squashfs /path/to/archives

# Specify output file location
tars2squashfs /path/to/archives -o /output/dataset.sqfs
```

### Advanced Usage

```bash
# Use fast compression for better read performance during training
tars2squashfs /data/archives -o dataset.sqfs -c lz4

# Memory-efficient mode for systems with limited inodes
tars2squashfs /data/archives --memory-efficient --batch-size 500

# Use local scratch space for temporary files
tars2squashfs /data/archives -o /project/dataset.sqfs --temp-dir /local/scratch

# Test run without creating files
tars2squashfs /data/archives -o /scratch/test.sqfs --dry-run

# Verbose output to monitor progress and file size growth
tars2squashfs /data/archives -o dataset.sqfs -v
```

### Command Line Options

```
usage: tars2squashfs [-h] [-o OUTPUT] [-b BATCH_SIZE] [-c {gzip,lzo,xz,lz4,zstd}] 
                     [--memory-efficient] [--temp-dir TEMP_DIR] [--dry-run] [-v] 
                     input_dir

Convert tar.gz archives to SquashFS sequentially

positional arguments:
  input_dir             Directory containing tar.gz files

options:
  -h, --help            show this help message and exit
  -o OUTPUT, --output OUTPUT
                        Output SquashFS file path (default: dataset.sqfs)
  -b BATCH_SIZE, --batch-size BATCH_SIZE
                        Number of files to process before appending (default: 1000)
  -c {gzip,lzo,xz,lz4,zstd}, --compression {gzip,lzo,xz,lz4,zstd}
                        Compression algorithm (default: xz)
  --memory-efficient    Use ultra memory-efficient mode (slower but uses minimal inodes)
  --temp-dir TEMP_DIR   Temporary directory (default: system temp)
  --dry-run             Show what would be done without creating files
  -v, --verbose         Enable verbose output showing file size growth
```

## Mounting SquashFS Files

### Without Root Privileges

```bash
# Create mount point
mkdir -p /path/to/mountpoint

# Mount the SquashFS file
squashfuse dataset.sqfs /path/to/mountpoint

# Access files
ls /path/to/mountpoint

# Unmount when done
fusermount -u /path/to/mountpoint
```

### With Root Privileges

```bash
# Mount
sudo mount -t squashfs dataset.sqfs /path/to/mountpoint

# Unmount
sudo umount /path/to/mountpoint
```

## Performance Considerations

### Compression Algorithm Choice

- **lz4**: Fastest decompression, larger file size (default, recommended for ML training)
- **xz**: Best compression ratio, slower read/write (default)
- **zstd**: Good balance of compression and speed
- **gzip**: Widely compatible, moderate performance
- **lzo**: Fast compression/decompression, moderate file size

### Memory Usage Optimization

- Use `--memory-efficient` for systems with limited inodes
- Adjust `--batch-size` based on available memory (lower = less memory usage)
- Use `--temp-dir` to specify fast local storage for temporary files

### Typical Use Cases

```bash
# High-performance ML training (fast reads)
tars2squashfs /data/training -o training.sqfs -c lz4 -b 2000

# Storage-optimized archival (maximum compression)
tars2squashfs /data/archive -o archive.sqfs -c xz -b 500

# Memory-constrained environments
tars2squashfs /data/small -o small.sqfs --memory-efficient -b 100
```


### Debug Mode

Enable verbose logging to diagnose issues:

```bash
tars2squashfs /data/archives -o test.sqfs -v --dry-run
```

## License

MIT License - see LICENSE file for details.
