#!/usr/bin/env perl


my %times;
my @keys;

while(my $line = <>) {
   next unless ($line =~ /^((.*):?)?\s+Time since the last reset\s+: (\d+\.\d+) s/);
   my $key = $2;
   my $time = $3;

   $times{$key}=0 unless exists $times{$key};
   $times{$key} += $time;
   push @keys, $key;
}

foreach my $key (@keys) {
   next unless exists $times{$key};
   print $key . " " . $times{$key} . "\n";
   delete $times{$key};
}
