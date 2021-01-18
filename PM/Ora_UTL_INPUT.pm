# Ora_UTL_INPUT.pm
#
# 2019-04-28
# Input like Oracle exp utility (cmd-line parameters param=value with parfile (parameter filename)

package Ora_UTL_INPUT ;

use strict ;
use warnings ;
use integer ;
use Exporter ;

@Ora_DIFF2::ISA    = qw(Exporter) ;
@Ora_DIFF2::EXPORT = qw(&basename &read_inputs) ;

# input parameters example (from cmd-line or from parameters file)
# my ( $uid,
#      $fce,
#      $prompt_flg,
#      $tab1,
#      $tab2,
#      $q1,
#      $q2,
#      $ra_cols_key_in,
#      $ra_cols_add_in,
#      $ra_cols_dif_in,
#      $ra_cols_omt_in
#    ) ;

use constant {
  # @a_params
  PN1 => 0, # parameter name 1 (long)
  PN2 => 1, # parameter name 2 (short)
  VTP => 2, # target variable type (s-string, a-array)
  MAN => 3, # mandatory (m), optional (o)
  REF => 4, # reference to target variable
  DES => 5, # parameter descriprion
  # $rh_pst
  IX  => 0, # param.index in @a_params
  OCU => 1  # param.occurence number
} ;

# input parameters description and reference to target variable example
# (description - constants for @a_params before)
# my @a_params = (
#   ['userid',  'uid',  's','m',\$uid,           'Oracle connect string'],
#   ['table1',  'tab1', 's','m',\$tab1,          'Primary table name <[owner.]table_name>'],
#   ['table2',  'tab2', 's','o',\$tab2,          'Secondary table name <[owner.]table_name>'],
#   ['query1',  'q1',   's','o',\$q1,            'optional where clause for primary table name'],
#   ['query2',  'q2',   's','o',\$q1,            'optional where clause for secondary table name'],
#   ['cols_key','c_key','a','m',\$ra_cols_key_in,'unique columns (for join clause)'],
#   ['cols_add','c_add','a','o',\$ra_cols_add_in,'added columns to KEY DIFF CNT sql (fce=1)'],
#   ['cols_omt','c_omt','a','o',\$ra_cols_omt_in,'omitted columns (fce=2,3)'],
#   ['cols_dif','c_dif','a','o',\$ra_cols_dif_in,'detail diff. columns (fce=3)'],
#   ['fce',     undef,  's','m',\$fce,           'generated SQL type: 1 - KEY DIFF CNT, 2 - COLS DIFF CNT, 3 - COLS DIFF DETAIL'],
#   ['prompt',  undef,  's','o',\$prompt_flg,    'if true (!=0) generate sqlplus prompt command instead of comment']
# ) ;
#
# Usage example:
#
# if( read_inputs( \@ARGV, \@a_params ) == 0 )
#   {
#    print '>> uid      = '. ((defined $uid) ? $uid : '')."\n" ;
#    print '>> cols_key = '. ((defined $ra_cols_key_in) ? join(', ', @$ra_cols_key_in ) : '') ."\n" ;
#   }

sub in_prepare
  # derive internal hashes from input reference to list $ra_params
  {
   my ( $ra_params ) = @_ ;
   my ( $ix, $cnt, $ra_par, $par1, $par2 ) ;
   my ( %h_p1, %h_pst ) ;

   $cnt = scalar @$ra_params ;
   for( $ix = 0 ; $ix < $cnt ; $ix++ )
     {
      $ra_par = $ra_params->[$ix] ;
      $par1 = (defined $ra_par->[ PN1 ]) ? uc( $ra_par->[ PN1 ] ) : '' ;
      $par2 = (defined $ra_par->[ PN2 ]) ? uc( $ra_par->[ PN2 ] ) : '' ;
      if( $par2 )
        {
         if( exists $h_p1{ $par2 } ) { print STDERR 'Error: duplicity on parameter name 2 ('. $par2 .").\n" }
         else { $h_p1{ $par2 } = $par1 } # mapping only (from short param name to long param name)
        }
      if( $par1 )
        {
         if( exists $h_pst{ $par1 } ) { print STDERR 'Error: duplicity on parameter name 1 ('. $par1 .").\n" }
         else { $h_pst{ $par1 } = [ $ix, 0 ] } # index to $ra_params, occurrence number init state
        }
      else
        { print STDERR 'Error: empty parameter name 1 for ix='. $ix ."\n" }
     }
   return ( \%h_p1, \%h_pst ) ;
  }


sub print_param_description
  {
   my ( $ra_params ) = @_ ;
   my $ra_par ;

   print STDERR 'Parameters description:'."\n".
                'Keyword    Man  Description'."\n".
                '-' x 60 ."\n" ;
   for $ra_par ( @$ra_params )
     {
      print STDERR sprintf "%-10s %-3s  %s\n", $ra_par->[ PN1 ], ($ra_par->[ MAN ] eq 'm') ? '*':' ', $ra_par->[ DES ] ;
     }
   print STDERR "\n" ;
   print STDERR sprintf "%-10s %-3s  %s\n", 'parfile',' ','Parameters filename' ;
   print STDERR sprintf "%-10s %-3s  %s\n", 'help',' ','Print parameter description' ;
   print STDERR "\n" ;
  }


sub process_param_value
  # process parameter value (from cmd-line or from parameters file)
  #   $pn  - parameter name
  #   $val - parameter value
  {
   my ( $pn, $val, $ra_params, $rh_p1, $rh_pst ) = @_ ;
   my ( $ra_pst, $pname, $par_ix, $par_ocu, $vtp, $refv ) ;
   my @a_val ;

   $pname = ( exists $rh_p1->{ $pn } ) ? $rh_p1->{ $pn } : $pn ;
   if( exists $rh_pst->{ $pname } )
     {
      $par_ix  = $rh_pst->{ $pname }[ IX ] ;
      $par_ocu = $rh_pst->{ $pname }[ OCU ] ;
      if( $par_ocu > 0 )                    { print STDERR 'Error: ambiguous occurrence of parameter name ('. $pname .")\n" }
      elsif( ! defined $val || $val eq '' ) { print STDERR 'Error: empty value for parameter name ('. $pname .")\n" }
      else
        {
         $refv = $ra_params->[ $par_ix ][ REF ] ; # reference to target variable
         $vtp  = $ra_params->[ $par_ix ][ VTP ] ; # target variable type
         if( $vtp eq 's' ) # string
           {
            $$refv = $val ; $rh_pst->{ $pname }[ OCU ] += 1 ;
           }
         else # list expected (values separated by ',')
           {
            $val =~ s/\s+//g ;
            if( $val =~ /^\(/ ) { $val =~ s/^\(// ; $val =~ s/\)$// ; }
            if( $val =~ /^'/ )  { $val =~ s/^'//  ; $val =~ s/'$//  ; }
            @a_val = split(/,/, $val ) ;
            if( scalar @a_val > 0 )
              {
               $$refv = \@a_val ; $rh_pst->{ $pname }[ OCU ] += 1 ;
              }
            else
              { print STDERR 'Error: empty value for parameter name ('. $pname .")\n" }
           }
        }
     }
   else
     { print STDERR 'Error: unknown parameter name ('. $pn .")\n" }
  }


sub process_param_file
  # process parameter file (skip comments, ...)
  {
   my ( $filename, $ra_params, $rh_p1, $rh_pst ) = @_ ;
   my ( $in, $row, $pn, $val ) ;

   if( ! open( $in, $filename ))
     { print STDERR 'Error on open('. $filename .'): '. $! ."\n\n" }
   else
     {
      while( $row = <$in> )
        {
         chomp( $row ) ;
         $row =~ s/\s*#.*$// ; # remove comment and whitespaces
         $row =~ s/^\s+// ;    # remove whitespaces at the beginning
         if( $row ne '' )
           {
            # ( $pn, $val ) = split(/\s*=\s*/, $row ) ;
            ( $pn, $val ) = $row =~ /^\s*(\w+)\s*=\s*(\S+.*)/ ;

            $pn = uc($pn) ;
            $val =~ s/\s+$// ; # remove whitespace at the end
            process_param_value( $pn, $val, $ra_params, $rh_p1, $rh_pst ) ;
           }
        }
      close( $in ) ;
     }
  }


sub mandatory_test
  {
   my ( $ra_params, $rh_pst ) = @_ ;
   my ( $pname, $par_ix, $par_ocu, $man, $err_cnt ) ;

   $err_cnt = 0 ;
   for $pname (sort keys %$rh_pst)
     {
      $par_ix  = $rh_pst->{ $pname }[ IX ] ;      # index at $ra_params
      $par_ocu = $rh_pst->{ $pname }[ OCU ] ;     # occurrence count
      $man     = $ra_params->[ $par_ix ][ MAN ] ; # mandatory flag
      if( $par_ocu == 0 && $man eq 'm')
        {
         ++$err_cnt ;
         print STDERR 'Error: mandatory parameter ('. $pname .') is not set.'."\n" ;
        }
     }
   return $err_cnt ; # errors number (0 : ok)
  }


sub basename
  {
   my ( $pathname ) = @_ ;
   my ( $pos1, $pos2, $pos ) ;

   $pos1 = rindex( $pathname,'\\') ;
   $pos2 = rindex( $pathname,'/') ;

   $pos = ( $pos1 > $pos2 ) ? $pos1 : $pos2 ;

   if( $pos > -1 )  { return substr( $pathname, $pos+1 ) }
   else             { return $pathname }
  }


sub read_inputs
  # read input parameters in "Oracle exp utility style" (parameter=value, parfile=<param_file> ...)
  {
   my ( $ra_argv, $ra_params ) = @_ ;
   my ( $cnt, $ix, $pn, $val ) ;
   my ( $rh_p1, $rh_pst ) ;
   my $rc = 0 ; # OK

   # internal hashes derived from $ra_params
   ( $rh_p1, $rh_pst ) = in_prepare( $ra_params ) ;

   # print '#P1: '. Dumper( $rh_p1 ) ;
   # print '#PST: '. Dumper( $rh_pst ) ;

   $cnt = scalar @$ra_argv ;
   if( $cnt == 0 )
     {
      $rc = -1 ;
      print STDERR 'No parameters.'."\n".'Parameters description - run:'."\n  ".
                    basename($0) .' help=y'."\n\n"
     }

   for( $ix = 0 ; $ix < $cnt ; ++$ix )
     {
      ( $pn, $val ) = split(/=/, $ra_argv->[$ix] ) ;
      $pn = uc( $pn ) ;
      if( $pn eq 'HELP') { $rc = -1 ; print_param_description( $ra_params ) ; last ; }
      elsif( $pn eq 'PARFILE')
        {
         if( defined $val && -f $val )
           {
            # print 'parfile = '. $val ."\n" ;
            process_param_file( $val, $ra_params, $rh_p1, $rh_pst ) ;
           }
         else
           { print STDERR 'Error: invalid value ('. $val .') for parameter '. $pn ."\n" }
        }
      else
        {
         process_param_value( $pn, $val, $ra_params, $rh_p1, $rh_pst ) ;
        }
     }
   if( $rc == 0 ) { $rc = mandatory_test( $ra_params, $rh_pst ) }
   return $rc ;
  }

1;

