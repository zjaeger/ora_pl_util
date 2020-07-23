#!/usr/bin/perl
# run_sql_diff_2.pl
#
# Purpose:
#   SQL code executor for generated code for COL.DIFF count
#
# Input parameters:
#   $Userid    - Oracle connect string
#   $Sql_fname - generated SQL sript (columns differences counts)

use strict ;
use integer ;
use DBI ;
use File::Basename ;
use Ora_LDA ;

# -- BEGIN --

$ENV{"NLS_SORT"} = 'BINARY' ;

my ( $Userid, $Sql_fname ) = get_input() ; # exit 1 on error
my $Lda = Ora_LDA::ora_LDA( $Userid ) ;

if( $Lda )
  {
   process_sql_file( $Sql_fname ) ;

   $Lda->disconnect() or warn "Disconnect: $DBI::errstr\n\n" ;
  }

# -- END --

# -----------------------------------------------------------------------------------
# PROCEDURE get_input()
# return ( $userid, $sql_fname ) ;
# Prevzeti parametru z "cmd.line"
# -----------------------------------------------------------------------------------

sub get_input
  {
   my ( $userid, $sql_fname ) ;
   my $val ;

   foreach $val ( @ARGV )
     {
      if( $val =~ /^[\w]+\/[\S]+@[\w\.]+$/
          || ( $val =~ /^\w+$/ && exists $ENV{ $val } ) )
        {
         if( ! defined( $userid )) { $userid = $val }
         else
           { print STDERR 'Unexpected value (userid): '. $val ."\n" }
        }
      else
        {
         if( -r $val )
           {
            if(    ! defined( $sql_fname )) { $sql_fname = $val }
            else
              { print STDERR 'Unexpected filename: '. $val ."\n" }
           }
         else
           { print STDERR 'Invalid filename: '. $val ."\n" }

        }
     }

   unless(    defined( $userid )
           && defined( $sql_fname ) )
     {
      printf STDERR
            'Invalid parameters'."\n".
            'Usage: '. basename( $0 ) .' <userid> <sql_file> > x_tab.csv'." \n\n" ;
      exit 1 ;
     }

   return ( $userid, $sql_fname ) ;
  }


sub process_sql_file
  {
   my ( $fname ) = @_ ;
   my ( $row, @a_val, $val, @a_tab, @a_col ) ;
   my $sql ;

   unless( open( IN, $fname )) { print "ERROR: can't open file " . $fname ."\n\n" ; return ; }
   ## open( IN, $fname ) || die "Can't open file " . $fname ."\n\n" ;
   while( $row = <IN> )
     {
      if( not $sql ) 
        {
         if( $row =~ /^--/ )
           {
            chomp( $row ) ;
            @a_val = split(' ', $row ) ;
            shift( @a_val ) ;
            $val = shift( @a_val ) ;
            if(    $val eq 'T' ) { @a_tab = @a_val }
            elsif( $val eq 'C' ) { @a_col = @a_val }
            next ;
           }

         if( uc( $row ) =~ /^(SELECT|WITH)/ )
           {
            $sql = $row ; next ;
           }
        }
      else
        {
         if( $row =~ /^\// )
           {
            ## print join('; ', @a_tab ) ."\n". join('; ', @a_col ) ."\n" ;
            ## print $sql ;
            execute_query( $sql, \@a_tab, \@a_col ) ;
            $sql = '' ;
           }
         else
           { $sql .= $row }
        }
     }

   close( IN ) ;
  }


sub execute_query
  {
   my ( $sql, $ra_tab, $ra_col ) = @_ ;
   my ( $tab_name1, $tab_name2 ) ;
   my ( @a_val, $col, $val, $index ) ;
   my $c1 ;

   ( $tab_name1, $tab_name2 ) = @$ra_tab ;

   print STDERR 'Tables: '. $tab_name1 .' x '. $tab_name2 ."\n" ;

   $c1 = $Lda->prepare( $sql ) ;
   $c1->execute() ; @a_val = $c1->fetchrow_array() ; $c1->finish() ;

   foreach $col ( @$ra_col )
     {
      $val = shift( @a_val ) ;
      print join(';', ( $tab_name1, $tab_name2, $col, $val )) ."\n" ;
     }
  }

# -- End of run_df_sql.pl

