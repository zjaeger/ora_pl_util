#!/usr/bin/perl
#
# get_pkgs.pl
#
# SCHEMA PACKAGES UPLOAD (into text-files):
#
# USAGE: get_pkgs.pl <oracle_connect_string>
# (oracle_connect_string: username/password@db_name)
#
# 2024-02-02 (last update)

use strict ;
use warnings ;
use integer ;
use File::Basename ;
use DBI ;
use Ora_LDA ;

# -- BEGIN --

$ENV{"NLS_SORT"} = 'BINARY' ;

my ( $Userid, $ra_Names ) = get_input_params( \@ARGV ) ;
my $Lda ;

# break flag variable
my $Sig_break = 0 ; $SIG{INT} = sub { $Sig_break = 1 } ;

# connect to Oracle DB
if( $Lda = Ora_LDA::ora_LDA( $Userid ))
  {
   save_label('00_db_plsql.lst') ;

   my $C_obj = $Lda->prepare( q{
select a.OBJECT_NAME
from   USER_OBJECTS a
where  a.OBJECT_TYPE = ? and regexp_like( a.OBJECT_NAME, ?,'i')
order by a.OBJECT_NAME
} ) ;

   my $C_src = $Lda->prepare( q{
select rtrim( a.TEXT )
from   USER_SOURCE a
where  a.NAME = ? and a.TYPE = ?
order by LINE
} ) ;

   for my $obj_name_RE ( @$ra_Names )
     {
      upload_pkg( $C_obj, $C_src, $obj_name_RE ) ;
      upload_trg( $C_obj, $C_src, $obj_name_RE ) ;
     }

   $Lda->disconnect() or warn "Disconnect: $DBI::errstr\n\n" ;
  }

# -- END --

sub get_input_params
  {
   my ( $ra_arg ) = @_ ;
   my ( $userid, # Oracle connect string
        @a_name  # view name regexp values
      ) ;

   @a_name = () ;
   foreach my $val ( @$ra_arg )
     {
      if( $val =~ /^[\w]+\/[\S]+@[\S]+$/ )
      #   || ( $val =~ /^\w+$/ && exists $ENV{ $val } )
        {
         if( ! defined( $userid )) { $userid = $val }
         else
           { print STDERR 'Unexpected value (userid): '. $val ."\n" }
        }
      else
        { push( @a_name, $val ) }
     }

   if( scalar @a_name == 0 ) { push( @a_name,'.*') }

   unless(    defined( $userid )
           && scalar @a_name != 0 )
     {
      if( scalar @$ra_arg > 0 ) { print STDERR 'Invalid parameters'."\n" }
      printf STDERR 'Usage: '. basename( $0 ) .' <userid> [<object_name_RE>] ...'."\n\n" ;
      exit 1 ;
     }

   return ( $userid, \@a_name ) ;
  }


sub save_label
  {
   my ( $fname ) = @_ ;
   my ( $sysdate, $uid, $out ) ;

   $sysdate = Ora_LDA::get_sysdate() ;
   $uid     = Ora_LDA::get_uid( $Lda ) ;

   open( $out,'>', $fname ) || die 'Error on open('. $fname .'): '. $! ."\n\n" ;
   print $out 'Date:   '. $sysdate ."\n".
              'Schema: '. $uid ."\n" ;
   close( $out ) ;

   print 'File '. $fname .' created.'."\n" ;
  }


sub upload_pkg
  {
   my ( $c_obj, $c_src, $obj_name_RE ) = @_ ;
   my ( $pname, $fname, $out ) ;
   my ( $text, $cnt ) ;

   $c_obj->execute('PACKAGE', $obj_name_RE ) ;
   while( ( $pname ) = $c_obj->fetchrow_array() )
     {
      $fname = lc( $pname ) .'.sql' ;
      open( $out,'>'. $fname ) or die "Can't open file ". $fname ."\n\n" ;
      print $out '-- '. $fname ."\n".
                 '--'."\n".
                 '-- rdbms: oracle'."\n\n" ;

    # print $out 'prompt >>> create package '. uc( $pname ) ."\n\n" ;

      print $out 'create or replace ' ;
      $c_src->execute( $pname,'PACKAGE') ;
      while( ( $text ) = $c_src->fetchrow_array() ) { if( $text ) { print $out $text } }

      print $out "\n/\n\n" ;
    # print $out 'prompt >>> create package body '. uc( $pname ) ."\n\n" ;

      $c_src->execute( $pname,'PACKAGE BODY') ;
      $cnt = 0 ;
      while( ( $text ) = $c_src->fetchrow_array() )
        {
         if( $cnt == 0 ) { print $out 'create or replace ' ; }
         ++$cnt ;
         print $out $text ;
        }
      if( $c_src->rows > 0 )
        {
         print $out "\n/\n" ;
       # print $out "show errors\n" ;
        }

      close( $out ) ;
      print 'File '. $fname .' created.'."\n" ;

      if( $Sig_break != 0 ) { print 'BREAK'."\n" ; last ; }
     }
  }


sub upload_trg
  {
   my ( $c_obj, $c_src, $obj_name_RE ) = @_ ;
   my ( $pname, $fname, $out, $text ) ;

   $c_obj->execute('TRIGGER', $obj_name_RE ) ;
   while( ( $pname ) = $c_obj->fetchrow_array() )
     {
      $fname = 'tr_'. lc( $pname ) .'.sql' ;
      open( $out, '>'. $fname ) or die "Can't open file: ". $fname ."\n\n" ;
      print $out '-- '. $fname ."\n\n" ;

    # print $out 'prompt >>> create trigger '. uc( $pname ) ."\n\n" ;

      print $out 'create or replace ' ;
      $c_src->execute( $pname,'TRIGGER') ;
      while( ( $text ) = $c_src->fetchrow_array() ) { print $out $text ; }

      print $out "/\n\n" ;

      close( $out ) ;
      print 'File '. $fname .' created.'."\n" ;

      if( $Sig_break != 0 ) { print 'BREAK'."\n" ; last ; }
     }
  }

# --- End of get_pkgs.pl

