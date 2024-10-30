package PVE::AutoBalloon;

use warnings;
use strict;

sub bytes_to_mb {
    my ($bytes) = @_;
    
    # 将字节转换为 MB
    my $mb = $bytes / (1024 * 1024);
    
    # 格式化为字符串，保留两位小数
    return sprintf("%.2f MB", $mb);
}

sub round_up_to_size {
    my ($bytes, $size_mb) = @_;
    $size_mb //= 128;  # 默认大小为128MB
    my $size = $size_mb * 1024 * 1024;  # 转换为字节
    return int(($bytes + ($size - 1)) / $size) * $size;
}

sub compute_alg1 {
    my ($vmstatus, $goal, $maxchange, $debug) =  @_;

    my $log = sub { print @_ if $debug; };

    my $change_func = sub {
	my ($res, $idlist, $bytes) = @_;

	my $rest = $bytes;
	my $repeat = 1;
	my $done_hash = {};
	my $progress = 1;

	while ($rest && $repeat && $progress) {
	    $repeat = 0;
	    $progress = 0;

	    my $shares_total = 0;
	    my $alloc_old = 0;

	    foreach my $vmid (@$idlist) {
		next if defined($done_hash->{$vmid});
		my $d = $vmstatus->{$vmid};
		my $balloon = defined($res->{$vmid}) ? $res->{$vmid} : $d->{balloon};
		$alloc_old += $balloon - $d->{balloon_min};
		$shares_total += $d->{shares} || 1000;
	    }

	    my $changes = 0;

	    my $alloc_new = $alloc_old + $rest;

	    &$log("shares_total: $shares_total $alloc_new\n");

	    foreach my $vmid (@$idlist) {
		next if defined($done_hash->{$vmid});
		my $d = $vmstatus->{$vmid};

		####################################
		my $used_memory = $d->{balloon} - $d->{freemem}; 
		my $free_memory = $d->{freemem};
		my $max_memory = $d->{maxmem};   
		my $free_percentage = $free_memory / $max_memory;

		# The desired balloon value is the free_percentage as 25%
		my $desired;
		if ($free_percentage > 0.3 ) {
			$desired = $d->{balloon} * 0.75;
		} elsif ($free_percentage < 0.2){
			$desired = $max_memory * 0.75;
		} else {
			next;  
		}
		&$log("vm $vmid : free_percentage " . bytes_to_mb($free_memory) . " / " . bytes_to_mb($max_memory) . " => " . sprintf("%.2f", $free_percentage) . ", desired: " . bytes_to_mb($desired) . " / " . bytes_to_mb($d->{balloon}) . "\n");

		####################################
		
		if ($desired > $d->{maxmem}) {
		    $desired = $d->{maxmem};
		    $repeat = 1;
		} elsif ($desired < $d->{balloon_min}) {
		    $desired = $d->{balloon_min};
		    $repeat = 1;
		}

		my ($new, $balloon);
		if (($bytes > 0) && ($desired - $d->{balloon}) > 0) { # grow
		    $new = $d->{balloon} + $maxchange;
		    $balloon = $new > $desired ? $desired : $new;
		} elsif (($desired - $d->{balloon}) < 0) { # shrink
		    $new = $d->{balloon} - $maxchange;
		    $balloon = $new > $desired ? $new : $desired;
		} else {
		    $done_hash->{$vmid} = 1;
		    next;
		}

		my $diff = $balloon - $d->{balloon};
		if ($diff != 0) {
		    my $oldballoon = defined($res->{$vmid}) ? $res->{$vmid} : $d->{balloon};
		    $res->{$vmid} = $balloon;
		    my $change = $balloon - $oldballoon;
		    if ($change != 0) {
			$changes += $change;
			my $absdiff = $diff > 0 ? $diff : -$diff;
			$progress += $absdiff;
			$repeat = 1;
		    }
		    &$log("change request for $vmid ($balloon, $diff, $desired, $new, $changes, $progress)\n");
		}
	    }

	    $rest -= $changes;
	}

	return $rest;
    };


    my $idlist = []; # list of VMs with working balloon river
    my $idlist1 = []; # list of VMs with memory pressure
    my $idlist2 = []; # list of VMs with enough free memory

    foreach my $vmid (keys %$vmstatus) {
	my $d = $vmstatus->{$vmid};
	next if !$d->{balloon}; # skip if balloon driver not running
	next if !$d->{balloon_min}; # skip if balloon value not set in config
	next if $d->{lock} &&  $d->{lock} eq 'migrate'; 
	next if defined($d->{shares}) && 
	    ($d->{shares} == 0); # skip if shares set to zero

	push @$idlist, $vmid;

	if ($d->{freemem} &&
	    ($d->{freemem} > $d->{balloon_min}*0.25) &&
	    ($d->{balloon} >= $d->{balloon_min})) {
	    push @$idlist2, $vmid;
	    &$log("idlist2 $vmid $d->{balloon}, $d->{balloon_min}, $d->{freemem}\n");
	} else {
	    push @$idlist1, $vmid;
	    &$log("idlist1 $vmid $d->{balloon}, $d->{balloon_min}\n");
	}
    }

    my $res = {};

    if ($goal > 10*1024*1024) {
	&$log("grow request start $goal\n");
	# priorize VMs with memory pressure
	my $rest = &$change_func($res, $idlist1, $goal);
	if ($rest >= $goal) { # no progress ==> consider all VMs
	    &$log("grow request loop $rest\n");
	    $rest = &$change_func($res, $idlist, $rest);
	}
	&$log("grow request end $rest\n");

    } elsif ($goal < -10*1024*1024) {
	&$log("shrink request $goal\n");
	# priorize VMs with enough free memory
	my $rest = &$change_func($res, $idlist2, $goal);
	if ($rest <= $goal) { # no progress ==> consider all VMs
	    &$log("shrink request loop $rest\n");
	    $rest = &$change_func($res, $idlist, $rest);
	}
	&$log("shrink request end $rest\n");
   } else {
	&$log("do nothing\n");
	# do nothing - requested change to small
    }

    foreach my $vmid (@$idlist) {
	next if !$res->{$vmid};
	my $d = $vmstatus->{$vmid};
	my $diff = int($res->{$vmid} - $d->{balloon});
	my $absdiff = $diff < 0 ? -$diff : $diff;
	# &$log("BALLOON $vmid to $res->{$vmid} ($diff)\n");
    }
    return $res;
}

1;
