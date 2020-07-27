#!/usr/bin/perl
#
# dbi_test.pl

use DBI ;

my @drivers = DBI->available_drivers() ;

die "No drivers found!\n" unless @drivers ;

foreach my $driver ( @drivers )
  {
   print "Driver: $driver\n",
   my @dataSources = DBI->data_sources( $driver ) ;
   foreach my $dataSource ( @dataSources )
     {
      print "\tData Source is $dataSource\n" ;
     }
   print "\n" ;
  }
