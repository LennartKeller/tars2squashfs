#!/usr/bin/env python3
"""
Sequential tar.gz to SquashFS converter
Optimized for minimal inode usage during processing
"""

import argparse
import logging
import os
import shutil
import subprocess
import sys
import tarfile
import tempfile
from contextlib import contextmanager
from pathlib import Path

from tqdm.auto import tqdm

logger = logging.getLogger(__name__)

# Constants
DEFAULT_BATCH_SIZE = 1000
PROGRESS_UPDATE_INTERVAL = 1000


class SquashFSBuilder:
    def __init__(self, output_file, batch_size=1000, compression='xz', temp_dir=None, temp_base=None, dry_run=False, merge_duplicates=True):
        self.output_file = Path(output_file).absolute()
        self.batch_size = batch_size
        self.compression = compression
        self.temp_dir = temp_dir
        self.temp_base = temp_base
        self.files_in_batch = 0
        self.total_files = 0
        self.current_batch_dir = None
        self.dry_run = dry_run
        self.merge_duplicates = merge_duplicates
        self.seen_top_dirs = set()  # Track top-level directories we've seen
        self.merge_base_dir = None  # Base directory for merging
        
    @contextmanager
    def temp_directory(self, prefix="squashfs_"):
        """Context manager for temporary directory"""
        if self.temp_dir is None:
            temp_dir = tempfile.mkdtemp(prefix=prefix, dir=self.temp_base)        
            try:
                temp_dir = Path(temp_dir)
                logger.debug(f"Created temporary directory: {temp_dir}")
                yield temp_dir
            finally:
                shutil.rmtree(temp_dir, ignore_errors=True)
        else:
            temp_dir = Path(self.temp_dir)
            if not temp_dir.exists():
                temp_dir.mkdir(parents=True, exist_ok=True)
            try:
                logger.debug(f"Created temporary directory: {temp_dir}")
                yield temp_dir
            finally:
                shutil.rmtree(temp_dir, ignore_errors=True)

    
    def check_tools(self):
        """Check if required tools are available"""
        try:
            subprocess.run(['mksquashfs', '-version'], 
                         capture_output=True, check=True)
        except (subprocess.CalledProcessError, FileNotFoundError):
            raise RuntimeError("mksquashfs not found. Please install squashfs-tools")
    
    def check_disk_space(self, required_space_mb=100):
        """Check if there's sufficient disk space"""
        if self.dry_run:
            return
        
        output_dir = self.output_file.parent
        try:
            stat = os.statvfs(output_dir)
            free_space_mb = (stat.f_bavail * stat.f_frsize) / (1024 * 1024)
            if free_space_mb < required_space_mb:
                raise RuntimeError(f"Insufficient disk space. Need at least {required_space_mb}MB, have {free_space_mb:.1f}MB")
        except AttributeError:
            # Windows doesn't have statvfs, skip check
            pass
    
    def initialize_squashfs(self):
        """Create initial empty squashfs file"""
        if self.dry_run:
            logger.info(f"[DRY RUN] Would initialize SquashFS file: {self.output_file}")
            return
            
        with self.temp_directory() as temp_dir:
            empty_dir = Path(temp_dir) / "empty"
            empty_dir.mkdir()
            
            cmd = ['mksquashfs', str(empty_dir), str(self.output_file), '-noappend']
            if self.compression:
                cmd.extend(['-comp', self.compression])
            
            subprocess.run(cmd, check=True, capture_output=True)
            logger.info(f"Initialized SquashFS file: {self.output_file}")
    
    def append_to_squashfs(self, source_dir):
        """Append a directory to the squashfs file"""
        if self.dry_run:
            file_count = sum(1 for _ in Path(source_dir).rglob('*') if _.is_file())
            logger.info(f"[DRY RUN] Would append {file_count} files from {source_dir}")
            return
        
        size_before = 0
        if logging.DEBUG >= logging.root.level:
            size_before = self.output_file.stat().st_size if self.output_file.exists() else 0
            
        # Check disk space before operation
        self.check_disk_space()
        
        cmd = ['mksquashfs', source_dir, str(self.output_file)]
        if self.compression:
            cmd.extend(['-comp', self.compression])
        
        try:
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
        except subprocess.TimeoutExpired:
            raise RuntimeError("mksquashfs operation timed out after 5 minutes")
        
        if result.returncode != 0:
            logger.error(f"mksquashfs error: {result.stderr}")
            if "No space left on device" in result.stderr:
                raise RuntimeError("Insufficient disk space for mksquashfs operation")
            raise RuntimeError(f"Failed to append to squashfs: {result.stderr}")
        
        if logging.DEBUG >= logging.root.level:
            size_after = self.output_file.stat().st_size
            if size_after < size_before:
                logger.warning(f"WARNING: SquashFS file size decreased! Was {size_before}, now {size_after}")
                logger.warning("This suggests the file was recreated instead of appended to.")
            else:
                size_diff = size_after - size_before
                logger.debug(f"  SquashFS grew by {size_diff / 1024 / 1024:.1f} MB (total: {size_after / 1024 / 1024:.1f} MB)")
    
    def get_top_level_dir(self, member_path):
        """Get the top-level directory from a tar member path"""
        if not member_path or member_path == '.':
            return None
        parts = Path(member_path).parts
        if not parts:
            return None
        # Skip if it's just a filename without directory
        if len(parts) == 1 and '.' in parts[0]:
            return None
        return parts[0]
    
    def setup_merge_directory(self, extract_path, top_dir):
        """Setup directory structure for merging duplicate top-level directories"""
        if not self.merge_duplicates or not top_dir:
            return extract_path
        
        if self.merge_base_dir is None:
            self.merge_base_dir = extract_path / "merged"
            if not self.dry_run:
                self.merge_base_dir.mkdir(exist_ok=True)
        
        # Create the target directory in the merge base
        target_dir = self.merge_base_dir / top_dir
        if not self.dry_run:
            target_dir.mkdir(exist_ok=True)
        
        return self.merge_base_dir
    
    def process_tar_member(self, tar, member, extract_path, top_dir=None):
        """Process a single member from tar archive"""
        if member.isfile():
            if not self.dry_run:
                if self.merge_duplicates and top_dir:
                    # Ensure merge directory is set up
                    if self.merge_base_dir is None:
                        self.merge_base_dir = extract_path / "merged"
                        self.merge_base_dir.mkdir(exist_ok=True)
                    
                    # Extract to temporary location first
                    temp_extract = extract_path / "temp_extract"
                    temp_extract.mkdir(exist_ok=True)
                    tar.extract(member, path=temp_extract, filter=tarfile.data_filter)
                    
                    # Move to merged location
                    src = temp_extract / member.name
                    dst = self.merge_base_dir / member.name
                    dst.parent.mkdir(parents=True, exist_ok=True)
                    
                    if src.exists():
                        if dst.exists():
                            logger.debug(f"  Overwriting duplicate file: {member.name}")
                        shutil.move(str(src), str(dst))
                    
                    # Clean up temp directory
                    shutil.rmtree(temp_extract, ignore_errors=True)
                else:
                    tar.extract(member, path=extract_path, filter=tarfile.data_filter)
            
            self.files_in_batch += 1
            self.total_files += 1
            
            if self.files_in_batch >= self.batch_size:
                logger.debug(f"  Appending batch of {self.files_in_batch} files...")
                if not self.dry_run:
                    append_path = self.merge_base_dir if self.merge_duplicates and self.merge_base_dir else extract_path
                    self.append_to_squashfs(append_path)
                    
                    # Reset merge directory if we're merging
                    if self.merge_duplicates and self.merge_base_dir:
                        shutil.rmtree(self.merge_base_dir)
                        self.merge_base_dir = extract_path / "merged"
                        self.merge_base_dir.mkdir(exist_ok=True)
                    else:
                        shutil.rmtree(extract_path)
                        os.makedirs(extract_path)
                self.files_in_batch = 0
    
    def process_archive_streaming(self, archive_path):
        """Process a tar.gz archive with streaming extraction"""
        logger.info(f"Processing: {archive_path.name}")
        
        with self.temp_directory(prefix="extract_") as extract_dir:
            self.files_in_batch = 0
            archive_top_dirs = set()
            
            # Reset merge_base_dir for each archive to use this archive's temp space
            if self.merge_duplicates:
                self.merge_base_dir = None
            
            try:
                with tarfile.open(archive_path, 'r:gz') as tar:
                    # First pass: identify top-level directories
                    if self.merge_duplicates:
                        for member in tar:
                            if member.isfile() or member.isdir():
                                top_dir = self.get_top_level_dir(member.name)
                                if top_dir:
                                    archive_top_dirs.add(top_dir)
                        
                        # Check for duplicates and log merge operations
                        duplicate_dirs = archive_top_dirs.intersection(self.seen_top_dirs)
                        if duplicate_dirs:
                            logger.info(f"  Merging duplicate directories: {', '.join(duplicate_dirs)}")
                        
                        # Update seen directories
                        self.seen_top_dirs.update(archive_top_dirs)
                        
                        # Reset tar file position for second pass
                        tar.close()
                        tar = tarfile.open(archive_path, 'r:gz')
                    
                    file_count = 0
                    pbar = tqdm(desc=f"Processing {archive_path.name}", unit="file", dynamic_ncols=True)
                    
                    for member in tar:
                        if member.isfile():
                            file_count += 1
                            top_dir = self.get_top_level_dir(member.name) if self.merge_duplicates else None
                            # Setup merge directory if we have a valid top_dir
                            if self.merge_duplicates and top_dir:
                                self.setup_merge_directory(extract_dir, top_dir)
                            self.process_tar_member(tar, member, extract_dir, top_dir)
                            pbar.update(1)
                            
                            if logging.DEBUG >= logging.root.level and file_count % PROGRESS_UPDATE_INTERVAL == 0:
                                logger.debug(f"    Processed {file_count} files...")
                                if self.output_file.exists():
                                    current_size = self.output_file.stat().st_size / 1024 / 1024
                                    logger.debug(f"    Current SquashFS size: {current_size:.1f} MB")
                    
                    pbar.close()
                    logger.info(f"  Found {file_count} files in archive")
                    
                    if self.files_in_batch > 0:
                        logger.debug(f"  Appending final batch of {self.files_in_batch} files...")
                        append_path = self.merge_base_dir if self.merge_duplicates and self.merge_base_dir else extract_dir
                        self.append_to_squashfs(append_path)
                        
            except tarfile.TarError as e:
                logger.error(f"Error reading tar file {archive_path}: {e}")
                raise RuntimeError(f"Corrupted or invalid tar file: {archive_path}")
            except Exception as e:
                logger.error(f"Unexpected error processing {archive_path}: {e}")
                raise
    
    def process_archive_memory_efficient(self, archive_path):
        """Ultra memory-efficient processing - extracts one file at a time"""
        logger.info(f"Processing (memory-efficient mode): {archive_path.name}")
        
        with self.temp_directory(prefix="batch_") as batch_dir:
            self.files_in_batch = 0
            archive_top_dirs = set()
            
            # First pass: identify top-level directories if merging
            if self.merge_duplicates:
                with tarfile.open(archive_path, 'r:gz') as tar:
                    for member in tar:
                        if member.isfile() or member.isdir():
                            top_dir = self.get_top_level_dir(member.name)
                            if top_dir:
                                archive_top_dirs.add(top_dir)
                
                # Check for duplicates and log merge operations
                duplicate_dirs = archive_top_dirs.intersection(self.seen_top_dirs)
                if duplicate_dirs:
                    logger.info(f"  Merging duplicate directories: {', '.join(duplicate_dirs)}")
                
                # Update seen directories
                self.seen_top_dirs.update(archive_top_dirs)
            
            with tarfile.open(archive_path, 'r:gz') as tar:
                pbar = tqdm()
                for member in tar:
                    if member.isfile():
                        if not self.dry_run:
                            # Get top directory for this member
                            top_dir = self.get_top_level_dir(member.name) if self.merge_duplicates else None
                            
                            if self.merge_duplicates and top_dir:
                                # Setup merge directory structure
                                if self.merge_base_dir is None:
                                    self.merge_base_dir = batch_dir / "merged"
                                    self.merge_base_dir.mkdir(exist_ok=True)
                                
                                temp_extract = Path(batch_dir) / "temp_extract"
                                temp_extract.mkdir(exist_ok=True)
                                
                                tar.extract(member, path=temp_extract, filter=tarfile.data_filter)
                                
                                src = temp_extract / member.name
                                dst = self.merge_base_dir / member.name
                                dst.parent.mkdir(parents=True, exist_ok=True)
                                
                                if dst.exists():
                                    logger.debug(f"  Overwriting duplicate file: {member.name}")
                                shutil.move(str(src), str(dst))
                                
                                shutil.rmtree(temp_extract)
                            else:
                                temp_extract = Path(batch_dir) / "temp_extract"
                                temp_extract.mkdir(exist_ok=True)
                                
                                tar.extract(member, path=temp_extract, filter=tarfile.data_filter)
                                
                                src = temp_extract / member.name
                                dst = Path(batch_dir) / member.name
                                dst.parent.mkdir(parents=True, exist_ok=True)
                                shutil.move(str(src), str(dst))
                                
                                shutil.rmtree(temp_extract)
                        
                        pbar.update(1)
                        self.files_in_batch += 1
                        self.total_files += 1
                        
                        if self.files_in_batch >= self.batch_size:
                            logger.debug(f"  Appending batch of {self.files_in_batch} files...")
                            append_path = self.merge_base_dir if self.merge_duplicates and self.merge_base_dir else batch_dir
                            self.append_to_squashfs(append_path)
                            if not self.dry_run:
                                if self.merge_duplicates and self.merge_base_dir:
                                    shutil.rmtree(self.merge_base_dir)
                                    self.merge_base_dir = batch_dir / "merged"
                                    self.merge_base_dir.mkdir(exist_ok=True)
                                else:
                                    shutil.rmtree(batch_dir)
                                    os.makedirs(batch_dir)
                            self.files_in_batch = 0
                
                if self.files_in_batch > 0:
                    logger.debug(f"  Appending final batch of {self.files_in_batch} files...")
                    append_path = self.merge_base_dir if self.merge_duplicates and self.merge_base_dir else batch_dir
                    self.append_to_squashfs(append_path)
    
    def build_from_archives(self, archive_list, memory_efficient=False):
        """Build squashfs from list of archives"""
        if not self.dry_run:
            self.check_tools()
        self.initialize_squashfs()
        
        total_archives = len(archive_list)
        
        for i, archive in enumerate(archive_list, 1):
            logger.info(f"[{i}/{total_archives}] Processing archive...")
            
            if memory_efficient:
                self.process_archive_memory_efficient(archive)
            else:
                self.process_archive_streaming(archive)
        
        logger.info(f"Successfully processed {self.total_files} files from {total_archives} archives")
        if self.dry_run:
            logger.info(f"[DRY RUN] Would create output file: {self.output_file}")
        else:
            logger.info(f"Output file: {self.output_file} ({self.output_file.stat().st_size / 1024 / 1024:.1f} MB)")


def find_archives(directory):
    """Find all tar.gz archives in directory"""
    archive_extensions = {'.tar.gz', '.tgz'}
    archives = []
    
    for file in Path(directory).iterdir():
        if "temp" in file.name:
            logger.debug(f"Skipping temporary file: {file.name}")
            continue
        if any(file.name.endswith(ext) for ext in archive_extensions):
            archives.append(file)
    
    return sorted(archives)


def main():
    parser = argparse.ArgumentParser(
        description='Convert tar.gz archives to SquashFS sequentially',
        epilog='''
Examples:
  # Basic usage with default output in current directory
  %(prog)s /data/archives
  
  # Specify output file in different directory
  %(prog)s /data/archives -o /scratch/user/datasets/audio.sqfs
  
  # Use local scratch for temp files and memory efficient mode
  %(prog)s /data/archives -o /project/dataset.sqfs --temp-dir /local/scratch --memory-efficient
  
  # Fast compression for better read performance during training
  %(prog)s /data/archives -o dataset.sqfs -c lz4
  
  # Test run without creating files
  %(prog)s /data/archives -o /scratch/test.sqfs --dry-run
  
  # Verbose output to monitor file size growth
  %(prog)s /data/archives -o dataset.sqfs -v
        ''',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument('input_dir', help='Directory containing tar.gz files')
    parser.add_argument('-o', '--output', default='dataset.sqfs', 
                        help='Output SquashFS file path (can be absolute or relative, default: dataset.sqfs)')
    parser.add_argument('-b', '--batch-size', type=int, default=DEFAULT_BATCH_SIZE,
                        help=f'Number of files to process before appending (default: {DEFAULT_BATCH_SIZE})')
    valid_compressions = ['gzip', 'lzo', 'xz', 'lz4', 'zstd']
    parser.add_argument('-c', '--compression', choices=valid_compressions,
                        default='lz4', help='Compression algorithm (default: xz)')
    parser.add_argument('--memory-efficient', action='store_true',
                        help='Use ultra memory-efficient mode (slower but uses minimal inodes)')
    parser.add_argument('--no-merge-duplicates', action='store_true',
                        help='Disable merging of duplicate top-level directories (default: merge enabled)')
    parser.add_argument('--temp-dir', help='Temporary directory (default: system temp)')
    parser.add_argument('--dry-run', action='store_true',
                        help='Show what would be done without creating the squashfs file')
    parser.add_argument('-v', '--verbose', action='store_true',
                        help='Enable verbose output showing file size growth')
    
    args = parser.parse_args()
    
    # Configure logging based on verbosity
    log_level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(level=log_level, format='%(asctime)s - %(levelname)s - %(message)s')
    
    # Validate compression choice
    if args.compression not in valid_compressions:
        logger.error(f"Invalid compression: {args.compression}. Valid options: {', '.join(valid_compressions)}")
        sys.exit(1)
    
    # Find archives
    archives = find_archives(args.input_dir)
    if not archives:
        logger.error(f"No tar.gz archives found in {args.input_dir}")
        sys.exit(1)
    
    logger.info(f"Found {len(archives)} archives to process")
    
    output_path = Path(args.output).absolute()
    output_dir = output_path.parent
    if not args.dry_run:
        if not output_dir.exists():
            logger.info(f"Creating output directory: {output_dir}")
            output_dir.mkdir(parents=True, exist_ok=True)
        
        if not os.access(output_dir, os.W_OK):
            logger.error(f"Output directory is not writable: {output_dir}")
            sys.exit(1)
    
    logger.info("Configuration:")
    logger.info(f"  Input directory: {args.input_dir}")
    logger.info(f"  Output file: {output_path}")
    logger.info(f"  Batch size: {args.batch_size}")
    logger.info(f"  Compression: {args.compression}")
    logger.info(f"  Mode: {'memory-efficient' if args.memory_efficient else 'streaming'}")
    logger.info(f"  Merge duplicates: {'disabled' if args.no_merge_duplicates else 'enabled'}")
    if args.temp_dir:
        logger.info(f"  Temp directory: {args.temp_dir}")
    if args.dry_run:
        logger.info("  DRY RUN MODE - No files will be created")
    
    # Build squashfs
    builder = SquashFSBuilder(
        output_file=output_path,
        batch_size=args.batch_size,
        compression=args.compression,
        temp_dir=args.temp_dir,
        dry_run=args.dry_run,
        merge_duplicates=not args.no_merge_duplicates
    )
    
    try:
        builder.build_from_archives(archives, memory_efficient=args.memory_efficient)
        
        if not args.dry_run:
            # Log mounting instructions
            logger.info("\nTo mount without root privileges:")
            logger.info(f"  squashfuse {output_path} /path/to/mountpoint")
            logger.info("\nTo unmount:")
            logger.info("  fusermount -u /path/to/mountpoint")
        else:
            logger.info("[DRY RUN] No files were created.")
        
    except Exception as e:
        logger.error(f"Failed to build SquashFS: {e}")
        sys.exit(1)