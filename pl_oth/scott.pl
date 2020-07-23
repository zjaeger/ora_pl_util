#!/usr/bin/perl -w

use strict ;
use DBI ;

my $lda = DBI->connect('dbi:Oracle:int-beq','scott','tiger')
          or die "Connect: $DBI::errstr\n\n" ;
my $c1 ;
my ( $no, $name, $job, $mgr ) ;
my $index ;

if( ($c1 = $lda->prepare("
SELECT empno, ename, job, mgr FROM emp ORDER BY empno"))
    && $c1->execute() )
  {
   $index = 0 ;
   while( ( $no, $name, $job, $mgr ) = $c1->fetchrow_array() )
     {
      ++$index ;
      printf "%d: %s|%s|%s|%s\n", $index, $no, $name, $job, $mgr ;
     }
  }

$lda->disconnect() or warn "Disconnect: $DBI::errstr\n\n" ;

