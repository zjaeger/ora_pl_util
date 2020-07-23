# tab_to_csv.pl
#
# CSV output file for selected tables

use strict ;
use warnings ;
use integer ;
use DBI ;

require 'ora_connect.pl' ;

# -- BEGIN --

$ENV{"NLS_SORT"} = 'BINARY' ;
unless( $ARGV[ 0 ] ) { die "No args (connect string expected).\n\n" }

my $Lda = ora_connect( $ARGV[ 0 ] ) ;

if( $Lda )
  {
   save_label() ;
   alter_session() ;

   my $c_tabs = $Lda->prepare("select TABLE_NAME from USER_TABLES WHERE TABLE_NAME like 'X1%' order by 1") ;
   my $Sysdate_YYYYMMDD = sysdate_YYYYMMDD() ;
   my $table_name ;

   $c_tabs->execute() ;
   while( ( $table_name ) = $c_tabs->fetchrow_array() )
     {
      export_to_csv( $table_name, $Sysdate_YYYYMMDD ) ;
     }

   $Lda->disconnect() or warn "Disconnect: $DBI::errstr\n\n" ;
  }

# -- END --

sub alter_session
  {
   my $c_set = $Lda->prepare("alter session set NLS_DATE_FORMAT='YYYY-MM-DD HH24:MI:SS'") ;
   $c_set->execute() ;
  }


sub sysdate
  {
   my @cas   = localtime(time) ;
   my $datum = sprintf "%02d.%02d.%4d %02d:%02d:%02d",
                       $cas[3], $cas[4]+1, $cas[5]+1900,
                       $cas[2], $cas[1], $cas[0] ;
   return $datum ;
  }


sub sysdate_YYYYMMDD
  {
   my @cas   = localtime(time) ;

   return sprintf "%4d%02d%02d", $cas[5]+1900, $cas[4]+1, $cas[3] ;
  }


sub get_uid
  {
   my ( $uname, $db_name, $db_domain ) ;
   my $c1 = $Lda->prepare("\
SELECT username,
       UPPER( SYS_CONTEXT('userenv','db_name')),
       SYS_CONTEXT('userenv','db_domain')
FROM   user_users") ;

   $c1->execute() ;
   ( $uname, $db_name, $db_domain ) = $c1->fetchrow_array() ;
   $c1->finish() ;

   return $uname .'@'. $db_name .'.'. $db_domain ;
  }


sub save_label
  {
   my ( $sysdate, $uid, $fname ) ;

   $sysdate = sysdate() ;
   $uid     = get_uid() ;

   $fname = 'tab_to_csv.label' ;

   open( OUT, ">$fname") || die "Can't open file ". $fname ."\n\n" ;
   print OUT 'Date:   '. $sysdate ."\n".
             'Schema: '. $uid ."\n" ;
   close( OUT ) ;

   print 'File '. $fname .' created.'."\n" ;
  }


sub export_to_csv
  {
   my ( $table_name, $sysdate_YYYYMMDD ) = @_ ;
   my ( $col_name, $data_type, $data_scale ) ;
   my @C_name ;     # -- column names
   my @C_type ;     # -- column type
   my @C_expr_out ; # -- column output expression

   my $c_col = $Lda->prepare("
SELECT column_name, data_type, data_scale
FROM   user_tab_columns
WHERE  table_name = ?
ORDER BY column_id") ;

   $c_col->execute( $table_name ) ;

   while( ( $col_name, $data_type, $data_scale ) = $c_col->fetchrow_array() )
     {
      push( @C_name, $col_name ) ;

      if(    $data_type eq 'DATE')
        {
         push( @C_expr_out,'to_char('. $col_name .",'YYYYMMDD')") ;
         push( @C_type,'D') ;
        }
      elsif( $data_type eq 'NUMBER')
        {
         push( @C_expr_out,'to_char('. $col_name .')') ;
         push( @C_type,'N') ;
        }
      elsif( $data_type =~ /CHAR/ )
        {
         push( @C_expr_out, $col_name ) ;
         push( @C_type,'C') ;
        }
      else
        {
         print 'Warning - column skipped: '. $table_name.'.'. lc( $col_name ) .' ('. $data_type .').'."\n" ;
         push( @C_expr_out,'NULL') ;
         push( @C_type,'x') ;
        }
     }

   if( @C_name > 0 )
     {
      my ( $fname, $sql, $c_out, $ra_val, $index, $value ) ;

      $fname = $table_name .'_'. $sysdate_YYYYMMDD .'.csv' ;
      print '>>> '. $fname ."\n" ;
      open( OUT,'>'. $fname ) or die "Can't open ". $fname .':'. $! ."\n\n" ;
      print OUT join(';', @C_name ) ."\n" ;

      # -- table export into file

      $sql = 'select '. join(', ', @C_expr_out ) .' from '. $table_name ;
      print $sql ."\n" ;
      $c_out = $Lda->prepare( $sql ) ;
      $c_out->execute() ;
    # $c_out->dump_results( 80,"\n",";", \*OUT ) ;
      while( $ra_val = $c_out->fetchrow_arrayref() )
        {
         for( $index = 0 ; $index < scalar @C_type ; ++$index )
           {
            $value = (defined( $ra_val->[ $index ] )) ? $ra_val->[ $index ] : '' ;

            if(    $C_type[ $index ] eq 'C') { print OUT '"'.$value.'";' }
            elsif( $C_type[ $index ] eq 'x') { print OUT '"?";' }
            else                             { print OUT $value .';' }
           }
         print OUT "\n" ;
        }
     }
   else
     { print STDERR 'ERROR: no data found for table_name='. $table_name ."\n" }

   close( OUT ) ;
  }
