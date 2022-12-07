require "xattr"

STATUS_UPDATE_GAP = 5
REBALANCE_XATTR = "trusted.distribute.migrate-data"
BLOCK_SIZE = 4096

class Rebalancer
  def initialize(@backend_dir : String, @mount_dir : String, @ignore_paths = [".glusterfs"])
    @total_bytes = 0_i64
    @scanned_bytes = BLOCK_SIZE.to_i64
    @start_time = Time.monotonic
    @last_status_updated = Time.monotonic
    update_total_bytes
  end

  def update_total_bytes
    stdout = IO::Memory.new
    proc = Process.new("du", ["-s", "-B1", @backend_dir], output: stdout)
    status = proc.wait
    if status.success?
      # Example output
      # 41259008 dirname
      used, _ = stdout.to_s.strip.split()
      @total_bytes = used.strip.to_i64
    else
      STDERR.puts "Failed to get total used bytes. Backend dir=#{@backend_dir}"
    end
  end

  def add_scanned_bytes(val, nlinks)
    rem = val.remainder(BLOCK_SIZE)
    newval = val - rem + (rem == 0 ? 0 : BLOCK_SIZE)
    # nlinks - 1 -> For Kadalu/Gluster backend
    @scanned_bytes += (newval/(nlinks-1)).to_i64
  end

  def print_progress(final_update = false)
    # Percentage of completion: scanned_bytes/total_bytes
    percent_progress = @scanned_bytes * 100 / @total_bytes
    # Estimated Completion Time: duration*100/(Percentage of completion)
    duration = Time.monotonic - @start_time
    return if percent_progress == 0
    estimate = (duration.seconds * 100 / percent_progress) - duration.seconds
    estimate = 0 if estimate < 0

    if final_update || (Time.monotonic - @last_status_updated).seconds > STATUS_UPDATE_GAP
      @last_status_updated = Time.monotonic
      # TODO: Write to Status file(Write to temp file and rename)
      puts "Progress=#{percent_progress.to_i}%  Scanned=#{@scanned_bytes.humanize_bytes}/#{@total_bytes.humanize_bytes} Duration=#{duration.seconds}s  Estimate=#{estimate.round(0)}s"
    end
  end

  def crawl
    all_dirs = [Path.new("")]
    while dir = all_dirs.shift?
      Dir.each_child(Path.new(@backend_dir, dir)) do |entry|
        rel_path = Path.new(dir, entry)

        # TODO: subdirectories and files inside ignored dirs
        # size are not added to the total bytes
        next if @ignore_paths.includes?(rel_path.to_s)

        backend_full_path = Path.new(@backend_dir, rel_path)
        if File.directory?(backend_full_path)
          @scanned_bytes += BLOCK_SIZE
          all_dirs << rel_path
          next
        end

        mnt_full_path = Path.new(@mount_dir, rel_path)

        # Stat the file from the @backend_dir to check the size
        begin
          file_info = File.info(backend_full_path, follow_symlinks: false)
          file_size = file_info.size
        rescue ex : File::Error
          next if ex.os_error == Errno::ENOENT

          STDERR.puts "Failed to get info of the file. file=#{rel_path} Error=#{ex}"
          next
        end

        # Issue Trigger rebalance xattr
        begin
          XAttr.set(mnt_full_path.to_s, REBALANCE_XATTR, "1", no_follow: true)
        rescue ex : IO::Error
          # DHT raises EEXIST if rebalance is not required for a file
          # If file is deleted in after directory listing and before calling this setxattr
          if ex.os_error != Errno::EEXIST && ex.os_error != Errno::ENOENT
            STDERR.puts "Failed to trigger rebalance. file=#{rel_path} Error=#{ex}"
          end
        end

        # Increment if rebalance complete or rebalance not required
        # or if any other error.
        add_scanned_bytes(file_size, file_info.@stat.st_nlink)

        print_progress
      end
    end

    # Crawl complete, so update scanned_bytes to 100%
    @scanned_bytes = @total_bytes
    print_progress(true)
  end
end

reb = Rebalancer.new ARGV[0], ARGV[1]
reb.crawl
