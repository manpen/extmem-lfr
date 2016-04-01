#!/usr/bin/env perl
use warnings;
use strict;
use Data::Dumper;
 use Math::BigFloat; 

my $key_idx = $ARGV[0] or 0;
my $key_slack = $ARGV[1] or 0;
my @points;

my $last_key = -1234567890;

sub sum {
   my $in = shift;
   my $s = Math::BigFloat->new(0);
   for my $t (@{$in}) {
      $s += $t;
   }
   return $s;
}

sub mean {
   my $in = shift;
   return sum($in) / (scalar @{$in});
}

sub stddev {
   my $in = shift;
   my $mean = mean($in);
   my $s = Math::BigFloat->new(0);
   return 0 if (1 == @{$in});
   for my $t (@{$in}) {
      $s += ($t - $mean) * ($t - $mean);
   }
   return ($s / (@{$in} - 1))->bpow(0.5);
}

sub print_stats {
   my $key = shift;
   my $key_idx = shift;
   my $points = shift;

   return unless scalar @$points;

   print (scalar @{$points->[0]});

   for my $col (@$points) {
      printf("     %15.2f %5.2f", mean($col), stddev($col));
   }

   print "\n"
}

for my $line ( <STDIN> ) {
   $line =~ s/(#.*)?\n//;
   $line =~ s/\s\s+/ /g;
   next unless $line;
   my @fields = split(/ /, $line);

   if ($#fields < $key_idx) {
      print "# line does not contain key\n";
      next;
   }
   
   my $key = Math::BigFloat->new($fields[$key_idx]);

   if ( (!$key_slack and $last_key != $key) or ($key_slack and abs(($last_key - $key) / $key) > $key_slack)) {
      print_stats($key, $key_idx, \@points);
      @points = ();
   }

   for(my $i=0; $i < (scalar @fields); $i++) {
      push @points, [] if ($i >= (scalar @points));
      push @{$points[$i]},Math::BigFloat->new($fields[$i]);  # $fields[$i];
   }
   
   $last_key = $key;
}

print_stats($last_key, $key_idx, \@points);

