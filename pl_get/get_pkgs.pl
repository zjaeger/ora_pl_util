#!/usr/bin/perl
# get_pkgs.pl
#
# SCHEMA PACKAGES UPLOAD (into text-files):
#
# USAGE: get_pkgs.pl <oracle_connect_string>
# (oracle_connect_string: username/password@db_name)

use strict ;
use warnings ;
use integer ;
use DBI ;
use Ora_LDA ;

# -- BEGIN --

$ENV{"NLS_SORT"} = 'BINARY' ;

unless( $ARGV[ 0 ] ) { die "No args (connect string expected).\n\n" ; }

my $Lda = Ora_LDA::ora_LDA( $ARGV[ 0 ] ) ;

if( $Lda )
  {
   save_label() ;

   print 'LongReadLen: '. $Lda->{LongReadLen} ."\n" ;
   $Lda->{LongReadLen} = 4096 ;

   my $C_obj = $Lda->prepare("\
SELECT object_name
FROM   user_objects
WHERE  object_type = ?
ORDER BY object_name") ;

   my $C_src = $Lda->prepare("\
SELECT RTRIM( text )
FROM   user_source
WHERE  name = ? AND type = ?
ORDER BY line") ;

   upload_pkg( $C_obj, $C_src ) ;
   ## upload_trg( $C_obj, $C_src ) ;

   $Lda->disconnect() or warn "Disconnect: $DBI::errstr\n\n" ;
  }

# -- END --

sub save_label
  {
   my $sysdate = Ora_LDA::get_sysdate() ;
   my $uid     = Ora_LDA::get_uid( $Lda ) ;
   my $fname = '00_db_pkgs.lst' ;
   my $out ;

   open( $out, '>'. $fname ) || die "Can't open file ". $fname ."\n\n" ;
   print $out 'Date:   '. $sysdate ."\n".
             'Schema: '. $uid ."\n" ;
   close( $out ) ;

   print 'File '. $fname .' created.'."\n" ;
  }


sub upload_pkg
  {
   my ( $c_obj, $c_src ) = @_ ;
   my ( $pname, $fname ) ;
   my ( $out, $text, $cnt ) ;

   $c_obj->execute('PACKAGE') ;
   while( ( $pname ) = $c_obj->fetchrow_array() )
     {
      $fname = lc( $pname ) .'.pls' ;
      open( $out,'>'. $fname ) or die "Can't open file ". $fname ."\n\n" ;
      print $out '-- '. $fname ."\n\n" ;

      print $out 'PROMPT Package '. uc( $pname ) ."\n\n" ;

      print $out 'CREATE OR REPLACE ' ;
      $c_src->execute( $pname,'PACKAGE') ;
      while( ( $text ) = $c_src->fetchrow_array() ) { if( $text ) { print $out $text } }

      print $out "\n/\n\n" ;

      $c_src->execute( $pname,'PACKAGE BODY') ;
      $cnt = 0 ;
      while( ( $text ) = $c_src->fetchrow_array() )
        {
       # if( $c_src->rows == 1 ) { print $out 'CREATE OR REPLACE ' ; }
         if( $cnt == 0 ) { print $out 'CREATE OR REPLACE ' ; }
         ++$cnt ;
         print $out $text ;
        }

      if( $c_src->rows > 0 ) { print $out "\n/\nshow errors\n" ; }

      close( $out ) ;
      print 'File '. $fname .' created.'."\n" ;
     }
  }


sub upload_trg
  {
   my ( $c_obj, $c_src ) = @_ ;
   my ( $pname, $fname ) ;
   my ( $out, $text ) ;

   $c_obj->execute('TRIGGER') ;
   while( ( $pname ) = $c_obj->fetchrow_array() )
     {
      $fname = 'tr_'. lc( $pname ) .'.pls' ;
      open( $out, '>'. $fname ) or die "Can't open file: ". $fname ."\n\n" ;
      print $out '-- '. $fname ."\n\n" ;

      print $out 'PROMPT Trigger '. uc( $pname ) ."\n\n" ;

      print $out 'CREATE OR REPLACE ' ;
      $c_src->execute( $pname,'TRIGGER') ;
      while( ( $text ) = $c_src->fetchrow_array() ) { print $out $text ; }

      print $out "/\n\n" ;

      close( $out ) ;
      print 'File '. $fname .' created.'."\n" ;
     }
  }

# --- End of get_pkgs.pl

