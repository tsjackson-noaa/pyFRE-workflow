#
# $Id: FREExperiment.pm,v 18.1.2.21.4.1.2.2 2014/12/05 17:08:12 Seth.Underwood Exp $
# ------------------------------------------------------------------------------
# FMS/FRE Project: Experiment Management Module
# ------------------------------------------------------------------------------
# arl    Ver   18.1  Merged revision 18.0.2.1 onto trunk            March 10
# afy -------------- Branch 18.1.2 -------------------------------- March 10
# afy    Ver   1.00  Modify extractCheckoutInfo (add line numbers)  March 10
# afy    Ver   1.01  Modify createCheckoutScript (keep order)       March 10
# afy    Ver   2.00  Remove createCheckoutScript subroutine         May 10
# afy    Ver   2.01  Remove createCompileScript subroutine          May 10
# afy    Ver   3.00  Remove executable subroutine                   May 10
# arl    Ver   4.00  Modify extractCheckoutInfo (read property)     August 10
# afy    Ver   5.00  Modify extractCheckoutInfo (no CVSROOT)        August 10
# afy    Ver   6.00  Use new module FREMsg (symbolic levels)        January 11
# afy    Ver   6.01  Modify extractNodes (no 'required')            January 11
# afy    Ver   6.02  Modify extractValue (no 'required')            January 11
# afy    Ver   6.03  Modify extractComponentValue (no 'required')   January 11
# afy    Ver   6.04  Modify extractSourceValue (no 'required')      January 11
# afy    Ver   6.05  Modify extractCompileValue (no 'required')     January 11
# afy    Ver   6.06  Modify extractCheckoutInfo (hashes, checks)    January 11
# afy    Ver   6.07  Modify extractCompileInfo (hashes, checks)     January 11
# afy    Ver   6.08  Modify extractCompileInfo (make overrides)     January 11
# afy    Ver   6.09  Modify extractCompileInfo (libraries order)    January 11
# afy    Ver   7.00  Modify placeholdersExpand (check '$' presence) April 11
# afy    Ver   7.01  Add property subroutine (similar to FRE.pm)    April 11
# afy    Ver   7.02  Modify experimentDirsCreate (call property)    April 11
# afy    Ver   7.03  Modify experimentDirsVerify (call property)    April 11
# afy    Ver   7.04  Modify extractCheckoutInfo (call property)     April 11
# afy    Ver   7.05  Modify experimentCreate (don't pass '$fre')    April 11
# afy    Ver   8.00  Add dir subroutine                             May 11
# afy    Ver   8.01  Add stateDir subroutine                        May 11
# afy    Ver   8.02  Modify dir-returning subroutines (call dir)    May 11
# afy    Ver   9.00  Modify dir-returning subroutines (cosmetics)   May 11
# afy    Ver  10.00  Add extractRegressionRunInfo subroutine        November 11
# afy    Ver  10.01  Add extractProductionRunInfo subroutine        November 11
# afy    Ver  10.02  Add executable subroutine                      November 11
# afy    Ver  10.03  Add executableCanBeBuilt subroutine            November 11
# afy    Ver  10.04  Modify extractExecutable subroutine            November 11
# afy    Ver  11.00  Add extractRegressionLabels subroutine         January 12
# afy    Ver  12.00  Add sdtoutTmpDir subroutine                    February 12
# afy    Ver  13.00  Remove tmpDir subroutine                       March 12
# afy    Ver  14.00  Add regressionLabels utility                   June 12
# afy    Ver  14.01  Add extractOverrideParams utility              June 12
# afy    Ver  14.02  Add overrideRegressionNamelists utility        June 12
# afy    Ver  14.03  Add overrideProductionNamelists utility        June 12
# afy    Ver  14.04  Add MPISizeParameters utility                  June 12
# afy    Ver  14.05  Add regressionPostfix utility                  June 12
# afy    Ver  14.06  Modify extractNamelists (use FRENamelists.pm)  June 12
# afy    Ver  14.07  Modify extractRegressionLabels (suite, all)    June 12
# afy    Ver  14.08  Modify extractRegressionRunInfo (add option)   June 12
# afy    Ver  14.09  Modify extractProductionRunInfo                June 12
# afy    Ver  14.10  Modify extractRegressionRunInfo (run as key)   June 12
# afy    Ver  15.00  Modify extractTables (return undef on errors)  July 12
# afy    Ver  16.00  Modify extractShellCommands (no 'defined')     July 12
# afy    Ver  16.01  Modify regressionPostfix (add suffuxes)        July 12
# afy    Ver  17.00  Modify MPISizeParameters (fix concurrent)      August 12
# afy    Ver  18.00  Merge with 18.1.2.17.2.1                       February 13
# afy    Ver  19.00  Modify MPISizeParameters (generic version)     February 13
# afy    Ver  19.01  Modify regressionPostfix (generic version)     February 13
# afy    Ver  19.02  Modify extract*RunInfo (generic version)       February 13
# afy    Ver  20.00  Modify MPISizeParameters (compatibility mode)  April 13
# keo    Ver  20.01  Modify extractCheckoutInfo (/:/)               April 13
# afy    Ver  21.00  Modify MPISizeCompatible (remove ice/land)     April 13
# afy    Ver  21.01  Add MPISizeComponentEnabled (subcomponents)    April 13
# afy    Ver  21.02  Modify MPISizeParametersGeneric (call ^)       April 13
# afy    Ver  21.03  Modify MPISizeParametersCompatible (serials)   April 13
# ------------------------------------------------------------------------------
# Copyright (C) NOAA Geophysical Fluid Dynamics Laboratory, 2009-2013
# Designed and written by V. Balaji, Amy Langenhorst and Aleksey Yakovlev
#

package FREExperiment;

use strict;

use List::Util();

use FREDefaults();
use FREMsg();
use FRENamelists(); 
use FRETargets();
use FREUtil();

# //////////////////////////////////////////////////////////////////////////////
# ////////////////////////////////////////////////////////// Global Constants //
# //////////////////////////////////////////////////////////////////////////////

use constant DIRECTORIES => FREDefaults::ExperimentDirs();
use constant REGRESSION_SUITE => ('basic', 'restarts', 'scaling');

# //////////////////////////////////////////////////////////////////////////////
# ////////////////////////////////////////////////////////// Global Variables //
# //////////////////////////////////////////////////////////////////////////////

my %FREExperimentMap = ();

# //////////////////////////////////////////////////////////////////////////////
# ///////////////////////////////////////////////////////////////// Utilities //
# //////////////////////////////////////////////////////////////////////////////

my $experimentFind = sub($)
# ------ arguments: $expName
{
  my $e = shift;
  return (exists($FREExperimentMap{$e})) ? $FREExperimentMap{$e} : '';
};

my $experimentDirsCreate = sub($)
# ------ arguments: $object
{
  my $r = shift;
  foreach my $t (FREExperiment::DIRECTORIES)
  {
    my $dirName = $t . 'Dir';
    $r->{$dirName} = $r->property($dirName);
  }
};

my $experimentDirsVerify = sub($$)
# ------ arguments: $object $expName
{
  my ($r, $e) = @_;
  my $result = 1;
  my ($fre, @expNamed) = ($r->fre(), split(';', $r->property('FRE.directory.expNamed')));
  foreach my $t (FREExperiment::DIRECTORIES)
  {
    my $d = $r->{$t . 'Dir'};
    if ($d)
    {
      # --------------------------------------------- check presence of the experiment name in the directory
      if (scalar(grep($_ eq $t, @expNamed)) > 0)
      {
	unless (FREUtil::dirContains($d, $e) > 0)
	{
	  $fre->out(FREMsg::FATAL, "The '$t' directory ($d) doesn't contain the experiment name");
	  $result = 0;
	  last;
	}
      }
      # -------------------------------------------------------- check placement the directory on the filesystem
      my $pathsMapping = $r->property('FRE.directory.' . $t . '.paths.mapping');
      if ($pathsMapping)
      {
        chomp(my $groupName = qx(id -gn));
        my $paths = FREUtil::strFindByPattern($pathsMapping, $groupName);
	if ($paths)
	{
	  my $pathsForMatch = $paths;
	  $pathsForMatch =~ s/\$/\\\$/g;
	  if ($d !~ m/^$pathsForMatch$/)
	  {
	    my @paths = split('\|', $paths);
	    my $pathsForOut = join(', ', @paths);
            $fre->out(FREMsg::FATAL, "The '$t' directory ($d) can't be set up - it must be one of ($pathsForOut)");
	    $result = 0;
	    last;
	  }
        }
	else
	{
	  $fre->out(FREMsg::FATAL, "The external property 'directory.$t.paths.mapping' is defined as '$pathsMapping' - this syntax is invalid");
	  $result = 0;
	  last;
	}
      }
      else
      {
        my $roots = $r->property('FRE.directory.' . $t . '.roots');
	if ($roots)
	{
	  my $rootsForMatch = $roots;
	  $rootsForMatch =~ s/\$/\\\$/g;
	  if (scalar(grep("$d/" =~ m/^$_\//, split(';', $rootsForMatch))) == 0)
	  {
	    my @roots = split(';', $roots);
	    my $rootsForOut = join(', ',  @roots);
	    $fre->out(FREMsg::FATAL, "The '$t' directory ($d) can't be set up - it must be on one of ($rootsForOut) filesystems");
            $result = 0;
	    last;
	  }
	}
	else
	{
	  $fre->out(FREMsg::FATAL, "The '$t' directory isn't bound by external properties");
	  $result = 0;
	  last;
	}
      }
    }
    else
    {
      $fre->out(FREMsg::FATAL, "The '$t' directory is empty");
      $result = 0;
      last;
    }
  }
  return $result;
};

my $experimentCreate;
$experimentCreate = sub($$$)
# ------ arguments: $className $fre $expName
# ------ create the experiment chain up to the root
{
  my ($c, $fre, $e) = @_;
  my $exp = $experimentFind->($e);
  if (!$exp)
  {
    my @experiments = $fre->experimentNames($e);
    if (scalar(grep($_ eq $e, @experiments)) > 0)
    {
      my $r = {};
      bless $r, $c;
      # ---------------------------- populate object fields
      $r->{fre} = $fre;
      $r->{name} = $e;
      $r->{node} = $fre->experimentNode($e);
      # ------------------------------------------------------ create and verify directories
      $experimentDirsCreate->($r);
      unless ($experimentDirsVerify->($r, $e))
      {
	$fre->out(FREMsg::FATAL, "The experiment '$e' can't be set up because of a problem with directories");
	return '';
      }
      # ------------------------------------------------------------- find and create the parent if needed
      my $expParentName = $r->experimentValue('@inherit');
      if ($expParentName)
      {
	if (scalar(grep($_ eq $expParentName, @experiments)) > 0)
	{
          $r->{parent} = $experimentCreate->($c, $fre, $expParentName);
	}
	else
	{
	  $fre->out(FREMsg::FATAL, "The experiment '$e' inherits from non-existent experiment '$expParentName'");
	  return '';
	}
      }
      else
      {
        $r->{parent} = '';
      }
      # ----------------------------------------------------------------------- save the experiment
      $FREExperimentMap{$e} = $r;
      # ----------------------------------------- return the newly created object handle
      return $r;
    }
    else
    {
      # ------------------------------------- experiment doesn't exist
      $fre->out(FREMsg::FATAL, "The experiment '$e' doesn't exist");
      return '';
    }
  }
  else
  {
    # ---------------- experiment exists: return it
    return $exp;
  }
};

my $strMergeWS = sub($)
# ------ arguments: $string
# ------ merge all the workspaces to a single space
{
  my $s = shift;
  $s =~ s/(?:^\s+|\s+$)//gso;
  $s =~ s/\s+/ /gso;
  return $s;
};

my $strRemoveWS = sub($)
# ------ arguments: $string
# ------ remove all the workspaces
{
  my $s = shift;
  $s =~ s/\s+//gso;
  return $s;
};

my $rankSet;
$rankSet = sub($$$)
# ------ arguments: $refToComponentHash $refToComponent $depth 
# ------ recursively set and return the component rank
# ------ return -1 if loop is found
{
  my ($h, $c, $d) = @_;
  if ($d < scalar(keys %{$h}))
  {
    my @requires = split(' ', $c->{requires});
    if (scalar(@requires) > 0)
    {
      my $rank = 0; 
      foreach my $required (@requires)
      {
        my $refReq = $h->{$required};
	my $rankReq = (defined($refReq->{rank})) ? $refReq->{rank} : $rankSet->($h, $refReq, $d + 1);
        if ($rankReq < 0)
	{
	  return -1;
	}
	elsif ($rankReq > $rank)
	{
	  $rank = $rankReq;
	}
      }
      $rank++;
      $c->{rank} = $rank;
      return $rank;
    }
    else
    {
      $c->{rank} = 0;
      return 0;
    }
  }
  else
  {
    return -1;
  }
};
    
my $regressionLabels = sub($)
# ------ arguments: $object
{
  my $r = shift;
  my @regNodes = $r->extractNodes('runtime', 'regression');
  my @labels = map($r->nodeValue($_, '@label') || $r->nodeValue($_, '@name'), @regNodes);
  return grep($_ ne "", @labels);
};

my $regressionRunNode = sub($$)
# ------ arguments: $object $label
{
  my ($r, $l) = @_;
  my @regNodes = $r->extractNodes('runtime', 'regression[@label="' . $l . '" or @name="' . $l . '"]');
  return (scalar(@regNodes) == 1) ? $regNodes[0] : undef;
};

my $productionRunNode = sub($)
# ------ arguments: $object
{
  my $r = shift;
  my @prdNodes = $r->extractNodes('runtime', 'production');
  return (scalar(@prdNodes) == 1) ? $prdNodes[0] : undef;
};

my $extractOverrideParams = sub($$$)
# ------ arguments: $exp $mamelistsHandle $runNode 
{

  my ($r, $h, $n) = @_;
  my $fre = $r->fre();

  my $res = $r->nodeValue($n, '@overrideParams');
  $res .= ';' if ($res and $res !~ /.*;$/);

  my $atmosLayout = $r->nodeValue($n, '@atmos_layout');
  if ($atmosLayout)
  {
    $res .= "bgrid_core_driver_nml:layout=$atmosLayout;" if $h->namelistExists('bgrid_core_driver_nml');
    $res .= "fv_core_nml:layout=$atmosLayout;" if $h->namelistExists('fv_core_nml');
    $fre->out
    (
      FREMsg::WARNING,
      "Usage of the 'atmos_layout' attribute is deprecated; instead, use",
      "<run overrideParams=\"fv_core_nml:layout=$atmosLayout\" ...>",
      "or <run overrideParams=\"bgrid_core_driver_nml:layout=$atmosLayout\" ...>"
    );
  }

  my $zetacLayout = $r->nodeValue($n, '@zetac_layout');
  if ($zetacLayout)
  {
    $res .= "zetac_layout_nml:layout=$zetacLayout;";
    $fre->out
    (
      FREMsg::WARNING,
      "Usage of the 'zetac_layout' attribute is deprecated; instead, use",
      "<run overrideParams=\"zetac_layout_nml:layout=$zetacLayout;namelist:var=val;...\" ...>"
    );
  }

  my $iceLayout = $r->nodeValue($n, '@ice_layout');
  if ($iceLayout)
  {
    $res .= "ice_model_nml:layout=$iceLayout;";
    $fre->out
    (
      FREMsg::WARNING,
      "Usage of the 'ice_layout' attribute is deprecated; instead, use",
      "<run overrideParams=\"ice_model_nml:layout=$iceLayout;namelist:var=val;...\" ...>"
    );
  }

  my $oceanLayout = $r->nodeValue($n, '@ocean_layout');
  if ($oceanLayout)
  {
    $res .= "ocean_model_nml:layout=$oceanLayout;";
    $fre->out
    (
      FREMsg::WARNING,
      "Usage of the 'ocean_layout' attribute is deprecated; instead, use",
      "<run overrideParams=\"ocean_model_nml:layout=$oceanLayout;namelist:var=val;...\" ...>"
    );
  }

  my $landLayout = $r->nodeValue($n, '@land_layout');
  if ($landLayout)
  {
    $res .= "land_model_nml:layout=$landLayout;";
    $fre->out
    (
      FREMsg::WARNING,
      "Usage of the 'land_layout' attribute is deprecated; instead, use",
      "<run overrideParams=\"land_model_nml:layout=$landLayout;namelist:var=val;...\" ...>"
    );
  }

  return $res;

};

my $overrideRegressionNamelists = sub($$$)
# ------ arguments: $exp $namelistsHandle $runNode
{

  my ($r, $h, $n) = @_;
  my $fre = $r->fre();
  
  my $overrideTypeless = sub($$$)
  {
    my ($l, $v, $x) = @_;
    if ($h->namelistExists($l))
    {
      $h->namelistTypelessPut($l, $v, $x);
    }
    else
    {
      $h->namelistPut($l, "\t$v = $x");
    }
  };

  foreach my $nml (split(';', $extractOverrideParams->($r, $h, $n)))
  {
    my ($name, $var, $val) = split(/[:=]/, $nml);
    $name =~ s/\s*//g;
    $var =~ s/\s*//g;
    unless ($name and $var) {$fre->out(FREMsg::WARNING, "Got an empty namelist in overrideParams"); next}
    $fre->out(FREMsg::NOTE, "overrideParams from xml: $name:$var=$val");
    my $contentOld = $h->namelistGet($name);
    $fre->out(FREMsg::NOTE, "Original namelist: '$name'\n$contentOld");
    $overrideTypeless->($name, $var, $val);
    my $contentNew = $h->namelistGet($name);
    $fre->out(FREMsg::NOTE, "Overridden namelist: '$name'\n$contentNew");
  }

  return $h;

};

my $overrideProductionNamelists = sub($$)
# ------ arguments: $object $namelistsHandle
{

  my ($r, $h) = @_;

  my $overrideLayout = sub($$)
  {
    my ($l, $x) = @_;
    if ($h->namelistExists($l))
    {
      $h->namelistLayoutPut($l, 'layout', $x);
    }
    else
    {
      $h->namelistPut($l, "\tlayout = $x");
    }
  };

  my $atmosLayout = $r->extractValue('runtime/production/peLayout/@atmos');
  $overrideLayout->('bgrid_core_driver_nml', $atmosLayout) if $atmosLayout;
  $overrideLayout->('fv_core_nml', $atmosLayout) if $atmosLayout;

  my $zetacLayout = $r->extractValue('runtime/production/peLayout/@zetac');
  $overrideLayout->('zetacLayout_nml', $zetacLayout) if $zetacLayout;

  my $iceLayout = $r->extractValue('runtime/production/peLayout/@ice');
  $overrideLayout->('ice_model_nml', $iceLayout) if $iceLayout;

  my $oceanLayout = $r->extractValue('runtime/production/peLayout/@ocean');
  $overrideLayout->('ocean_model_nml', $oceanLayout) if $oceanLayout;

  my $landLayout = $r->extractValue('runtime/production/peLayout/@land');
  $overrideLayout->('land_model_nml', $landLayout) if $landLayout;

  return $h;

};

my $MPISizeCompatible = sub($$)
# ------ arguments: $fre $namelistsHandle
{
  my ($fre, $h) = @_;
  my $compatible = 1;
  my @components = split(';', $fre->property('FRE.mpi.component.names'));
  my @compatibleComponents = ('atmos', 'ocean');
  foreach my $component (@components)
  {
    unless (scalar(grep($_ eq $component, @compatibleComponents)) > 0) 
    {
      if (defined(FRENamelists::namelistBooleanGet($h, 'coupler_nml', "do_$component")))
      {
	$compatible = 0;
	last;
      }
    }
  }
  return $compatible;
};

my $MPISizeParametersCompatible = sub($$$$)
# ------ arguments: $exp $npes $namelistsHandle $ensembleSize
{

  my ($r, $n, $h, $s) = @_;
  
  my ($fre, $concurrent) = ($r->fre(), $h->namelistBooleanGet('coupler_nml', 'concurrent'));
  $concurrent = 1 unless defined($concurrent);
  
  my ($atmosNP, $atmosNT) = ($h->namelistIntegerGet('coupler_nml', 'atmos_npes') || 0, 1);
  my ($oceanNP, $oceanNT) = ($h->namelistIntegerGet('coupler_nml', 'ocean_npes') || 0, 1);

  if ($atmosNP < 0)
  {
    $fre->out(FREMsg::FATAL, "Number '$atmosNP' of atmospheric MPI processes must be non-negative");
    return undef;
  }
  elsif ($atmosNP > $n)
  {
    $fre->out(FREMsg::FATAL, "Number '$atmosNP' of atmospheric MPI processes must be less or equal than a total number '$n' of MPI processes");
    return undef;
  }

  if ($oceanNP < 0)
  {
    $fre->out(FREMsg::FATAL, "Number '$oceanNP' of oceanic MPI processes must be non-negative");
    return undef;
  }
  elsif ($oceanNP > $n)
  {
    $fre->out(FREMsg::FATAL, "Number '$oceanNP' of oceanic MPI processes must be less or equal than a total number '$n' of MPI processes");
    return undef;
  }

  if (FRETargets::containsOpenMP($fre->target()))
  {
    my $coresPerNode = $fre->property('FRE.scheduler.run.coresPerJob.inc');
    $atmosNT = $h->namelistIntegerGet('coupler_nml', 'atmos_nthreads') || 1;
    if ($atmosNT <= 0)
    {
      $fre->out(FREMsg::FATAL, "Number '$atmosNT' of atmospheric OpenMP threads must be positive");
      return undef;
    }
    elsif ($atmosNT > $coresPerNode)
    {
      $fre->out(FREMsg::FATAL, "Number '$atmosNT' of atmospheric OpenMP threads must be less or equal than a number '$coresPerNode' of cores per node");
      return undef;
    }
    $oceanNT = $h->namelistIntegerGet('coupler_nml', 'ocean_nthreads') || 1;
    if ($oceanNT <= 0)
    {
      $fre->out(FREMsg::FATAL, "Number '$oceanNT' of oceanic OpenMP threads must be positive");
      return undef;
    }
    elsif ($oceanNT > $coresPerNode)
    {
      $fre->out(FREMsg::FATAL, "Number '$oceanNT' of oceanic OpenMP threads must be less or equal than a number '$coresPerNode' of cores per node");
      return undef;
    }
  }
  
  if ($atmosNP > 0 || $oceanNP > 0)
  {
    my $ok = 1;
    my @npes = (); 
    my @ntds = ($atmosNT, $oceanNT);
    if ($atmosNP < $n && $oceanNP == 0)
    {
      @npes = ($concurrent) ? ($atmosNP * $s, ($n - $atmosNP) * $s) : ($n * $s, 0);
    }
    elsif ($atmosNP == 0 && $oceanNP < $n)
    {
      @npes = ($concurrent) ? (($n - $oceanNP) * $s, $oceanNP * $s) : ($n * $s, 0); 
    }
    elsif ($atmosNP == $n && $oceanNP == 0)
    {
      @npes = ($atmosNP * $s, 0); 
    }
    elsif ($atmosNP == 0 && $oceanNP == $n)
    {
      @npes = (0, $oceanNP * $s); 
    }
    elsif ($atmosNP == $n || $oceanNP == $n)
    {
      if ($concurrent)
      {
	$fre->out(FREMsg::FATAL, "Concurrent run - total number '$n' of MPI processes is equal to '$atmosNP' atmospheric ones OR to '$oceanNP' oceanic ones");
	$ok = 0;
      }
      else
      {
        @npes = ($n * $s, 0);
      }
    }
    elsif ($atmosNP + $oceanNP == $n)
    {
      @npes = ($atmosNP * $s, $oceanNP * $s); 
    }
    else
    {
      $fre->out(FREMsg::FATAL, "Total number '$n' of MPI processes isn't equal to the sum of '$atmosNP' atmospheric and '$oceanNP' oceanic ones");
      $ok = 0;
    }   
    return ($ok) ? {npes => $n * $s, coupler => 1, npesList => \@npes, ntdsList => \@ntds} : undef;
  }
  else
  {
    return {npes => $n * $s};
  }
  
};

my $MPISizeComponentEnabled = sub($$$)
# ------ arguments: $exp $namelistsHandle $componentName
{
  my ($r, $h, $n) = @_;
  my ($fre, $result) = ($r->fre(), undef);
  my @subComponents = split(';', $fre->property("FRE.mpi.$n.subComponents"));
  foreach my $component ($n, @subComponents)
  {
    my $enabled = $h->namelistBooleanGet('coupler_nml', "do_$component");
    if (defined($enabled))
    {
      if ($enabled)
      {
        $result = 1;
	last;
      }
      elsif (!defined($result))
      {
        $result = 0;
      }
    }
  }
  return $result;
};

my $MPISizeParametersGeneric = sub($$$$)
# ------ arguments: $exp $npes $namelistsHandle $ensembleSize
{
  my ($r, $n, $h, $s) = @_;
  my $pairSplit = sub($) {return split('<', shift)};
  my $pairJoin = sub($$) {return join('<', @_)};
  my $fre = $r->fre();
  my %sizes = ();
  my @enabled = split(';', $fre->property('FRE.mpi.component.enabled'));
  my @components = split(';', $fre->property('FRE.mpi.component.names'));
  my $openMPEnabled = FRETargets::containsOpenMP($fre->target());
  my $coresPerNode = ($openMPEnabled) ? $fre->property('FRE.scheduler.run.coresPerJob.inc') : undef;
  # ------------------------------------------------------------------------- determine component sizes 
  for (my $inx = 0; $inx < scalar(@components); $inx++)
  {
    my $component = $components[$inx];
    my $enabled = $MPISizeComponentEnabled->($r, $h, $component);
    $enabled = $enabled[$inx] unless defined($enabled);
    if ($enabled)
    {
      if (my $npes = $h->namelistIntegerGet('coupler_nml', "${component}_npes"))
      {
	$sizes{"${component}_npes"} = $npes * $s; 
	if ($openMPEnabled)
	{
          my $ntds = $h->namelistIntegerGet('coupler_nml', "${component}_nthreads");
	  unless (defined($ntds))
	  {
	    $sizes{"${component}_ntds"} = 1; 
	  }
	  elsif (0 < $ntds && $ntds <= $coresPerNode)
	  {
	    $sizes{"${component}_ntds"} = $ntds; 
	  }
	  elsif ($ntds <= 0)
	  {
            $fre->out(FREMsg::FATAL, "The variable 'coupler_nml:${component}_nthreads' must have a positive value");
	    return undef;
	  }
	  else
	  {
            $fre->out(FREMsg::FATAL, "The variable 'coupler_nml:${component}_nthreads' value must be less or equal than a number '$coresPerNode' of cores per node");
	    return undef;
	  }
	}
	else
	{
	  $sizes{"${component}_ntds"} = 1; 
	}
      }
      else
      {
        $fre->out(FREMsg::FATAL, "The variable 'coupler_nml:${component}_npes' must be defined and have a positive value");
	return undef;
      }
    }
    else
    {
      $sizes{"${component}_npes"} = 0;
      $sizes{"${component}_ntds"} = 1;
    }
  }
  # --------------------------------------------------- select enabled components (with positive sizes)  
  if (my @componentsEnabled = grep($sizes{"${_}_npes"} > 0, @components))
  {
    my %pairs = ();
    my @pairsAllowed = split(';', $fre->property('FRE.mpi.component.serials'));
    # -------------------------------- determine components pairing (for enabled components only)
    foreach my $componentL (@componentsEnabled)
    {
      foreach my $componentR (@componentsEnabled)
      {
	if ($h->namelistBooleanGet('coupler_nml', "serial_${componentL}_${componentR}"))
	{
	  my $pairCurrent = $pairJoin->($componentL, $componentR);
	  if (grep($_ eq $pairCurrent, @pairsAllowed))
	  {
	    my $componentLExtra = undef;
	    foreach my $pair (keys %pairs)
	    {
	      my ($componentLExisting, $componentRExisting) = $pairSplit->($pair);
	      $componentLExtra = $componentLExisting if $componentRExisting eq $componentR; 
	    }
	    unless ($componentLExtra)
	    {
	      $pairs{$pairCurrent} = 1;
	    }
	    else
	    {
              $fre->out(FREMsg::FATAL, "Components '$componentL' and '$componentR' can't be run serially - the '$componentLExtra' and '$componentR' are already configured to run serially");
	      return undef;
	    }
	  }
	  else
	  {
            $fre->out(FREMsg::FATAL, "Components '$componentL' and '$componentR' aren't allowed to run serially");
	    return undef;
	  }
	} 
      }
    }
    # ----------------------------------------------- modify component sizes according to their pairing
    while (my @pairs = keys %pairs)
    {
      my @pairComponentsL = map(($pairSplit->($_))[0], @pairs);
      foreach my $pair (@pairs)
      {
	my ($componentL, $componentR) = $pairSplit->($pair);
	unless (grep($_ eq $componentR, @pairComponentsL))
	{
	  $sizes{"${componentL}_npes"} = List::Util::max($sizes{"${componentL}_npes"}, $sizes{"${componentR}_npes"});
	  $sizes{"${componentL}_ntds"} = List::Util::max($sizes{"${componentL}_ntds"}, $sizes{"${componentR}_ntds"});
	  $sizes{"${componentR}_npes"} = 0;
	  delete $pairs{$pair};
	}
      }
    }
    # ----------------------------------------------- normal return
    my @npes = map($sizes{"${_}_npes"}, @components); 
    my @ntds = map($sizes{"${_}_ntds"}, @components); 
    return {npes => $n * $s, coupler => 1, npesList => \@npes, ntdsList => \@ntds};
  }
  else
  {
    my $componentsForOut = join(', ', @components);
    $fre->out(FREMsg::FATAL, "At least one of the components '$componentsForOut' must be configured to run");
    return undef;
  }
};

my $MPISizeParameters = sub($$$)
# ------ arguments: $exp $npes $namelistsHandle
{

  my ($r, $n, $h) = @_;
  my $fre = $r->fre();

  my $ensembleSize = $h->namelistIntegerGet('ensemble_nml', 'ensemble_size');
  $ensembleSize = 1 unless defined($ensembleSize);
  
  if ($ensembleSize > 0)
  {
    if ($h->namelistExists('coupler_nml'))
    {
      my $func = ($MPISizeCompatible->($fre, $h)) ? $MPISizeParametersCompatible : $MPISizeParametersGeneric;
      return $func->($r, $n, $h, $ensembleSize);
    }
    elsif ($n > 0)
    {
      return {npes => $n * $ensembleSize};
    }
    else
    {
      $fre->out(FREMsg::FATAL, "The <production> or <regression/run> attribute 'npes' must be defined and have a positive value");
      return undef;
    }
  }
  else
  {
    $fre->out(FREMsg::FATAL, "The variable 'ensemble_nml:ensemble_size' must have a positive value");
    return undef;
  }

};

my $regressionPostfix = sub($$$$$$$$$)
# ------ arguments: $exp $label $runNo $hoursFlag $segmentsNmb $monthsNmb $daysNmb $hoursNmb $mpiInfo 
{
  my ($r, $l, $i, $hf, $sn, $mn, $dn, $hn, $h) = @_;
  my ($fre, $timing, $size) = ($r->fre(), $sn . 'x' . $mn . 'm' . $dn . 'd', '');
  $timing .= $hn . 'h' if $hf;
  if ($h->{coupler})
  {
    my ($refNPes, $refNTds) = ($h->{npesList}, $h->{ntdsList});
    my @suffixes = split(';', $fre->property('FRE.mpi.component.suffixes'));
    for (my $inx = 0; $inx < scalar(@{$refNPes}); $inx++)
    {
      $size .= '_' . $refNPes->[$inx] . 'x' . $refNTds->[$inx] . $suffixes[$inx] if $refNPes->[$inx] > 0; 
    }
  }
  else
  {
    $size = '_' . $h->{npes} . 'pe';
  }
  return $timing . $size;
};

# //////////////////////////////////////////////////////////////////////////////
# ////////////////////////////////////////// Class initialization/termination //
# //////////////////////////////////////////////////////////////////////////////

sub new($$$)
# ------ arguments: $className $fre $expName
# ------ called as class method
# ------ creates an object and populates it 
{
  my ($c, $fre, $e) = @_;
  return $experimentCreate->($c, $fre, $e);
}

sub DESTROY
# ------ arguments: $object
# ------ called automatically
{
}

# //////////////////////////////////////////////////////////////////////////////
# //////////////////////////////////////////////////////////// Object methods //
# //////////////////////////////////////////////////////////////////////////////

sub fre($)
# ------ arguments: $object
# ------ called as object method
{
  my $r = shift;
  return $r->{fre};
}

sub name($)
# ------ arguments: $object
# ------ called as object method
{
  my $r = shift;
  return $r->{name};
}

sub node($)
# ------ arguments: $object
# ------ called as object method
{
  my $r = shift;
  return $r->{node};
}

sub parent($)
# ------ arguments: $object
# ------ called as object method
{
  my $r = shift;
  return $r->{parent};
}

sub dir($$)
# ------ arguments: $object $dirType
# ------ called as object method
{
  my ($r, $t) = @_;
  return $r->{$t . 'Dir'};
}

sub rootDir($)
# ------ arguments: $object
# ------ called as object method
{
  my $r = shift;
  return $r->dir('root');
}

sub srcDir($)
# ------ arguments: $object
# ------ called as object method
{
  my $r = shift;
  return $r->dir('src');
}

sub execDir($)
# ------ arguments: $object
# ------ called as object method
{
  my $r = shift;
  return $r->dir('exec');
}

sub scriptsDir($)
# ------ arguments: $object
# ------ called as object method
{
  my $r = shift;
  return $r->dir('scripts');
}

sub stdoutDir($)
# ------ arguments: $object
# ------ called as object method
{
  my $r = shift;
  return $r->dir('stdout');
}

sub stdoutTmpDir($)
# ------ arguments: $object
# ------ called as object method
{
  my $r = shift;
  return $r->dir('stdoutTmp');
}

sub stateDir($)
# ------ arguments: $object
# ------ called as object method
{
  my $r = shift;
  return $r->dir('state');
}

sub workDir($)
# ------ arguments: $object
# ------ called as object method
{
  my $r = shift;
  return $r->dir('work');
}

sub ptmpDir($)
# ------ arguments: $object
# ------ called as object method
{
  my $r = shift;
  return $r->dir('ptmp');
}

sub archiveDir($)
# ------ arguments: $object
# ------ called as object method
{
  my $r = shift;
  return $r->dir('archive');
}

sub postProcessDir($)
# ------ arguments: $object
# ------ called as object method
{
  my $r = shift;
  return $r->dir('postProcess');
}

sub analysisDir($)
# ------ arguments: $object
# ------ called as object method
{
  my $r = shift;
  return $r->dir('analysis');
}

sub placeholdersExpand($$)
# ------ arguments: $object $string
# ------ called as object method
# ------ expand all the experiment level placeholders in the given $string
{
  my ($r, $s) = @_;
  if (index($s, '$') >= 0)
  {
    my $v = $r->{name};
    $s =~ s/\$(?:\(name\)|\{name\}|name)/$v/g;
  }
  return $s;
}

sub property($$)
# ------ arguments: $object $propertyName
# ------ called as object method
# ------ return the value of the property $propertyName, expanded on the experiment level
{
  my ($r, $k) = @_;
  return $r->placeholdersExpand($r->fre()->property($k));
};

sub nodeValue($$$)
# ------ arguments: $object $node $xPath
# ------ called as object method
# ------ return $xPath value relative to the given $node 
{
  my ($r, $n, $x) = @_;
  return $r->placeholdersExpand($r->fre()->nodeValue($n, $x));
}

sub experimentValue($$)
# ------ arguments: $object $xPath
# ------ called as object method
# ------ return $xPath value relative to the experiment node 
{
  my ($r, $x) = @_;
  return $r->nodeValue($r->node(), $x);
}

sub description($)
# ------ arguments: $object
# ------ called as object method
# ------ returns the experiment description
{
  my $r = shift;
  return $r->experimentValue('description');
}

sub executable($)
# ------ arguments: $object
# ------ called as object method
# ------ return standard executable name for the given experiment
{
  my $r = shift;
  my ($execDir, $name) = ($r->execDir(), $r->name());
  return "$execDir/fms_$name.x";
}

sub executableCanBeBuilt($)
# ------ arguments: $object
# ------ called as object method
# ------ return 1 if the executable for the given experiment can be built
{
  my $r = shift;
  return
  (
    $r->experimentValue('*/source/codeBase') ne ''
    ||
    $r->experimentValue('*/source/csh') ne ''
    ||
    $r->experimentValue('*/compile/cppDefs') ne ''
    ||
    $r->experimentValue('*/compile/srcList') ne ''
    ||
    $r->experimentValue('*/compile/pathNames') ne ''
    ||
    $r->experimentValue('*/compile/csh') ne ''
  );       
}

# //////////////////////////////////////////////////////////////////////////////
# ////////////////////////////////////////// Data Extraction With Inheritance //
# //////////////////////////////////////////////////////////////////////////////

sub extractNodes($$$)
# ------ arguments: $object $xPathRoot $xPathChildren
# ------ called as object method
# ------ return a nodes list corresponding to the $xPathRoot/$xPathChildren, following inherits
# ------ if xPathRoot returns a list of nodes, only the first node will be taken into account
{
  my ($r, $x, $y) = @_;
  my ($exp, @results) = ($r, ());
  while ($exp and scalar(@results) == 0)
  {
    my $rootNode = $exp->node()->findnodes($x)->get_node(1);
    push @results, $rootNode->findnodes($y) if $rootNode;
    $exp = $exp->parent();
  }
  return @results;
}

sub extractValue($$)
# ------ arguments: $object $xPath
# ------ called as object method
# ------ return a value corresponding to the $xPath, following inherits
{
  my ($r, $x) = @_;
  my ($exp, $value) = ($r, '');
  while ($exp and !$value)
  {
    $value = $exp->experimentValue($x);
    $exp = $exp->parent();
  }
  return $value;
}

sub extractComponentValue($$$)
# ------ arguments: $object $xPath $componentName
# ------ called as object method
# ------ return a value corresponding to the $xPath under the <component> node, following inherits
{
  my ($r, $x, $c) = @_;
  my ($exp, $value) = ($r, '');
  while ($exp and !$value)
  {
    $value = $exp->experimentValue('component[@name="' . $c . '"]/' . $x);
    $exp = $exp->parent();
  }
  return $value;
}

sub extractSourceValue($$$)
# ------ arguments: $object $xPath $componentName
# ------ called as object method
# ------ return a value corresponding to the $xPath under the <component/source> node, following inherits
{
  my ($r, $x, $c) = @_;
  my ($exp, $value) = ($r, '');
  while ($exp and !$value)
  {
    $value = $exp->experimentValue('component[@name="' . $c . '"]/source/' . $x);
    $exp = $exp->parent();
  }
  return $value;
}

sub extractCompileValue($$$)
# ------ arguments: $object $xPath $componentName
# ------ called as object method
# ------ return a value corresponding to the $xPath under the <component/compile> node, following inherits
{
  my ($r, $x, $c) = @_;
  my ($exp, $value) = ($r, '');
  while ($exp and !$value)
  {
    $value = $exp->experimentValue('component[@name="' . $c . '"]/compile/' . $x);
    $exp = $exp->parent();
  }
  return $value;
}

sub extractExecutable($)
# ------ arguments: $object
# ------ called as object method
# ------ return predefined executable name (if found) and experiment object, following inherits
{

  my $r = shift;
  my ($exp, $fre, $makeSenseToCompile, @results) = ($r, $r->fre(), undef, ());

  while ($exp)
  {
    $makeSenseToCompile = $exp->executableCanBeBuilt();
    @results = $fre->dataFilesMerged($exp->node(), 'executable', 'file');
    last if scalar(@results) > 0 || $makeSenseToCompile;
    $exp = $exp->parent();
  }

  if (scalar(@results) > 0)
  {
    $fre->out(FREMsg::WARNING, "The executable name is predefined more than once - all the extra definitions are ignored") if scalar(@results) > 1;
    return (@results[0], $exp);
  }
  elsif ($makeSenseToCompile)
  {
    return (undef, $exp);
  }
  else
  {
    return (undef, undef);
  }

}

sub extractMkmfTemplate($$)
# ------ arguments: $object $componentName
# ------ called as object method
# ------ extracts a mkmf template, following inherits
{

  my ($r, $c) = @_;
  my ($exp, $fre, @results) = ($r, $r->fre(), ());

  while ($exp and scalar(@results) == 0)
  {
    my @nodes = $exp->node()->findnodes('component[@name="' . $c . '"]/compile');
    foreach my $node (@nodes) {push @results, $fre->dataFilesMerged($node, 'mkmfTemplate', 'file');}
    $exp = $exp->parent();
  }
  
  $fre->out(FREMsg::WARNING, "The '$c' component mkmf template is defined more than once - all the extra definitions are ignored") if scalar(@results) > 1;
  return @results[0];

}

sub extractDatasets($)
# ------ arguments: $object
# ------ called as object method
# ------ extracts file pathnames together with their target names, following inherits
{

  my $r = shift;
  my ($exp, $fre, @results) = ($r, $r->fre(), ());

  while ($exp and scalar(@results) == 0)
  {
    # --------------------------------------------- get the input node
    my $inputNode = $exp->node()->findnodes('input')->get_node(1);
    # ------------------------------------------------ process the input node
    if ($inputNode)
    {
      # ----------------------------------------------------- get <dataFile> nodes
      push @results, $fre->dataFiles($inputNode, 'input');
      # ----------------------------------------------------- get nodes in the old format
      my @nodesForCompatibility = $inputNode->findnodes('fmsDataSets');
      foreach my $node (@nodesForCompatibility)
      {
	my $sources = $exp->nodeValue($node, 'text()');
	my @sources = split(/\s+/, $sources);
	foreach my $line (@sources)
	{
          next unless $line;
	  if (substr($line, 0, 1) eq '/')
	  {
	    my @lineParts = split('=', $line);
	    if (scalar(@lineParts) > 2)
	    {
	      $fre->out(FREMsg::WARNING, "Too many names for renaming are defined at '$line' - all the extra names are ignored");
	    }
	    my ($source, $target) = @lineParts;
	    if ($target)
	    {
	      $target = 'INPUT/' . $target;
	    }
	    else
	    {
	      $target = FREUtil::fileIsArchive($source) ? 'INPUT/' : 'INPUT/.';
            }
            push @results, $source;
	    push @results, $target;
	  }
	  else
	  {
	    push @results, $line;
	    push @results, '';
	  }
	}
      }
    }
    # ---------------------------- repeat for the parent
    $exp = $exp->parent();
  } 

  return @results;

}

sub extractNamelists($)
# ------ arguments: $object
# ------ called as object method
# ------ following inherits, but doesn't overwrite existing hash entries
# ------ returns namelists handle
{

  my $r = shift;
  my ($exp, $fre, $nmls) = ($r, $r->fre(), FRENamelists->new());

  $fre->out(FREMsg::NOTE, "Extracting namelists...");
  
  while ($exp)
  {
    # -------------------------------------------- get the input node
    my $inputNode = $exp->node()->findnodes('input')->get_node(1);
    # ------------------------------------------------ process the input node
    if ($inputNode)
    {
      # ----------------------------------- get inline namelists (they take precedence)
      my @inlineNmlNodes = $inputNode->findnodes('namelist[@name]');        
      foreach my $inlineNmlNode (@inlineNmlNodes)
      {
	my $name = FREUtil::cleanstr($exp->nodeValue($inlineNmlNode, '@name'));
	my $content = $exp->nodeValue($inlineNmlNode, 'text()');
	$content =~ s/^\s*$//mg;
	$content =~ s/^\n//;
	$content =~ s/\s*(?:\/\s*)?$//;
	if ($nmls->namelistExists($name))
	{
	  my $expName = $exp->name();
          $fre->out(FREMsg::NOTE, "Using secondary specification of '$name' rather than the original setting in '$expName'");
	}
	elsif ($name)
	{
	  $nmls->namelistPut($name, $content);
	}
      }
      # --------------------------------------------------------------- get namelists from files
      my @nmlFiles = $fre->dataFilesMerged($inputNode, 'namelist', 'file');
      foreach my $filePath (@nmlFiles)
      {
        if (-f $filePath and -r $filePath)
	{
	  my $fileContent = qx(cat $filePath);
	  $fileContent =~ s/^\s*$//mg;
	  $fileContent =~ s/^\s*#.*$//mg;
	  $fileContent = $fre->placeholdersExpand($fileContent);
	  $fileContent = $exp->placeholdersExpand($fileContent);
	  my @fileNmls = split(/\/\s*$/m, $fileContent);
	  foreach my $fileNml (@fileNmls)
	  {
            $fileNml =~ s/^\s*\&//;
            $fileNml =~ s/\s*(?:\/\s*)?$//;
            my ($name, $content) = split('\s', $fileNml, 2);
	    if ($nmls->namelistExists($name))
	    {
              $fre->out(FREMsg::NOTE, "Using secondary specification of '$name' rather than the original setting in '$filePath'");
            }
	    elsif ($name)
	    {
	      $nmls->namelistPut($name, $content);
            }
	  }
        }
	else
	{
	  return undef;
	}
      }
    }
    # ---------------------------- repeat for the parent
    $exp = $exp->parent();
  }

  return $nmls;

}

sub extractTable($$)
# ------ arguments: $object $label
# ------ called as object method
# ------ returns data, corresponding to the $label table, following inherits 
{

  my ($r, $l) = @_;
  my ($exp, $fre, $value) = ($r, $r->fre(), '');

  # ------------------------------------------- get the input node
  my $inputNode = $exp->node()->findnodes('input')->get_node(1);
  # --------------------------------------------- process the input node
  if ($inputNode)
  {
    # ----------------- Find nodes that have the wrong @order attribute.
    my @inlineAppendTableNodes = $inputNode->findnodes($l . '[@order and not(@order="append")]');
    if (@inlineAppendTableNodes)
    {
      $fre->out(FREMsg::FATAL, "The value for attribute order in $l is not valid.");
      return undef;
    }
    # ----------------- get inline tables except for "@order="append"" (they must be before tables from files and appended nodes)
    my @inlineTableNodes = $inputNode->findnodes($l . '[not(@file) and not(@order="append")]');
    foreach my $inlineTableNode (@inlineTableNodes)
    {
      $value .= $exp->nodeValue($inlineTableNode, 'text()');
    }
    # --------------------------------------------------------------- get tables from files
    my @tableFiles = $fre->dataFilesMerged($inputNode, $l, 'file');
    foreach my $filePath (@tableFiles)
    {
      if (-f $filePath and -r $filePath)
      {
	my $fileContent = qx(cat $filePath);
	$fileContent = $fre->placeholdersExpand($fileContent);
	$fileContent = $exp->placeholdersExpand($fileContent);
	$value .= $fileContent;
      }
      else
      {
	return undef;
      }
    }
  }

  # ---------------------------- repeat for the parent
  if ($exp->parent() and !$value)
  {
    $value .= $exp->parent()->extractTable($l);
  }

  #  ---------------------------- now add appended tables
  if ($inputNode)
  {
    # ----------------- get [@order="append"] tables
    my @inlineAppendTableNodes = $inputNode->findnodes($l . '[@order="append"]');
    foreach my $inlineAppendTableNode (@inlineAppendTableNodes)
    {
      $value .= $exp->nodeValue($inlineAppendTableNode, 'text()');
    }
  }

  # ---------------------------- sanitize table
  $value =~ s/\n\s*\n/\n/sg;
  $value =~ s/^\s*\n\s*//s;
  $value =~ s/\s*\n\s*$//s;

  return $value;

}

sub extractShellCommands($$%)
# ------ arguments: $object $xPath %adjustment
# ------ called as object method
# ------ returns shell commands, corresponding to the $xPath, following inherits
# ------ adjusts commands, depending on node types
{

  my ($r, $x, %a) = @_;
  my ($exp, $value) = ($r, '');

  while ($exp and !$value)
  {
    my @nodes = $exp->node()->findnodes($x);
    foreach my $node (@nodes)
    {
      my $type = $exp->nodeValue($node, '@type');
      my $content = $exp->nodeValue($node, 'text()');
      if (exists($a{$type})) {$content = $a{$type}[0].$content.$a{$type}[1];}
      $value .= $content;
    }
    $exp = $exp->parent();
  }
    
  return $value;

}  

sub extractVariableFile($$)
# ------ arguments: $object $label
# ------ called as object method
# ------ returns filename for the $label variable, following inherits
{

  my ($r, $l) = @_;
  my ($exp, $fre, @results) = ($r, $r->fre(), ());

  while ($exp and scalar(@results) == 0)
  {
    my $inputNode = $exp->node()->findnodes('input')->get_node(1);
    push @results, $fre->dataFilesMerged($inputNode, $l, 'file') if $inputNode;
    $exp = $exp->parent();
  } 

  $fre->out(FREMsg::WARNING, "The variable '$l' is defined more than once - all the extra definitions are ignored") if scalar(@results) > 1;
  return @results[0];

}

sub extractReferenceFiles($)
# ------ arguments: $object
# ------ called as object method
# ------ return list of reference files, following inherits
{

  my $r = shift;
  my ($exp, $fre, @results) = ($r, $r->fre(), ());

  while ($exp and scalar(@results) == 0)
  {
    my $runTimeNode = $exp->node()->findnodes('runtime')->get_node(1);
    push @results, $fre->dataFilesMerged($runTimeNode, 'reference', 'restart') if $runTimeNode;
    $exp = $exp->parent();
  }
  
  return @results;

}

sub extractReferenceExperiments($)
# ------ arguments: $object
# ------ called as object method
# ------ return list of reference experiment names, following inherits
{
  my ($r, @results) = (shift, ());
  my @nodes = $r->extractNodes('runtime', 'reference/@experiment');
  foreach my $node (@nodes) {push @results, $r->nodeValue($node, '.');}
  return @results;
}

sub extractPPRefineDiagScripts($)
# ------ arguments: $object
# ------ called as object method
# ------ return list of postprocessing refine diagnostics scriptnames, following inherits 
{
  my ($r, @results) = (shift, ());
  my @nodes = $r->extractNodes('postProcess', 'refineDiag/@script');
  foreach my $node (@nodes) {push @results, $r->nodeValue($node, '.');}
  return @results;
}

sub extractCheckoutInfo($)
# ------ arguments: $object
# ------ called as object method
# ------ return a reference to checkout info, following inherits
{

  my $r = shift;
  my ($fre, $expName, @componentNodes) = ($r->fre(), $r->name(), $r->node()->findnodes('component'));
  
  if (scalar(@componentNodes) > 0)
  {
    my %components;
    foreach my $componentNode (@componentNodes)
    {
      my $name = $r->nodeValue($componentNode, '@name');
      if ($name)
      {
	$fre->out(FREMsg::NOTE, "COMPONENTLOOP ((($name)))");
	if (!exists($components{$name}))
	{
	  # ------------------------------------- get and check library data; skip the component if the library defined  
	  my $libraryPath = $r->extractComponentValue('library/@path', $name);
	  if ($libraryPath)
	  {
	    if (-f $libraryPath)
	    {
	      my $libraryHeaderDir = $r->extractComponentValue('library/@headerDir', $name);
	      if ($libraryHeaderDir)
	      {
		if (-d $libraryHeaderDir)
		{
        	  $fre->out(FREMsg::NOTE, "You have requested library '$libraryPath' for component '$name' - we will skip the component checkout");
        	  next;
		}
		else
		{
        	  $fre->out(FREMsg::FATAL, "Component '$name' specifies non-existent library header directory '$libraryHeaderDir'");
		  return 0;
		}
	      }
	      else
	      {
        	$fre->out(FREMsg::FATAL, "Component '$name' specifies library '$libraryPath' but no header directory");
        	return 0;
	      }
	    }
	    else
	    {
              $fre->out(FREMsg::FATAL, "Component '$name' specifies non-existent library '$libraryPath'");
	      return 0;
	    }	 
	  }
	  # ------------------------------------------------------------------------------- get and check component data for sources checkout
	  my $codeBase = $strMergeWS->($r->extractSourceValue('codeBase', $name));
	  if ($codeBase)
	  {
	    my $codeTag = $strRemoveWS->($r->extractSourceValue('codeBase/@version', $name));
	    if ($codeTag)
	    {
	      my $vcBrand = $strRemoveWS->($r->extractSourceValue('@versionControl', $name)) || 'cvs';
	      if ($vcBrand)
	      {
		my $vcRoot = $strRemoveWS->($r->extractSourceValue('@root', $name)) || $r->property('FRE.versioncontrol.cvs.root');
		if ($vcRoot =~ /:/ or (-d $vcRoot and -r $vcRoot))
		{
		  # ------------------------------------------------------------------------------------------ save component data into the hash
        	  my %component = ();
		  $component{codeBase} = $codeBase;
		  $component{codeTag} = $codeTag;
		  $component{vcBrand} = $vcBrand;
		  $component{vcRoot} = $vcRoot;
		  $component{sourceCsh} = $r->extractSourceValue('csh', $name);
        	  $component{lineNumber} = $componentNode->line_number();
		  # ----------------------------------------------------------------------------------------------- print what we got
		  $fre->out
		  (
		    FREMsg::NOTE,
		    "name           = $name",
		    "codeBase       = $component{codeBase}",
		    "codeTag        = $component{codeTag}",
		    "vcBrand        = $component{vcBrand}",
		    "vcRoot         = $component{vcRoot}",
		    "sourceCsh      = $component{sourceCsh}"
		  );
		  # -------------------------------------------------------------- link the component to the components hash
		  $components{$name} = \%component;
        	}
		else
		{
        	  $fre->out(FREMsg::FATAL, "Component '$name': the directory '$vcRoot' doesn't exist or not readable");
		  return 0;
		}
              }
	      else
	      {
        	$fre->out(FREMsg::FATAL, "Component '$name': element <source> doesn't specify a version control system");
		return 0;
	      }
	    }
	    else
	    {
              $fre->out(FREMsg::FATAL, "Component '$name': element <source> doesn't specify a version attribute for its code base");
	      return 0;
	    }
	  }
	  else
	  {
            $fre->out(FREMsg::FATAL, "Component '$name': element <source> doesn't specify a code base");
	    return 0;
	  }
	}
	else
	{
	  $fre->out(FREMsg::FATAL, "Component '$name' is defined more than once - make sure each component has a distinct name");
	  return 0;
	}
      }
      else
      {
	$fre->out(FREMsg::FATAL, "Components with empty names aren't allowed");
	return 0;
      }
    }
    return \%components;  
  }
  else
  {
    $fre->out(FREMsg::FATAL, "The experiment '$expName' doesn't contain any components");
    return 0;
  }

}

sub extractCompileInfo($)
# ------ arguments: $object
# ------ called as object method
# ------ return a reference to compile info
{

  my $r = shift;
  my ($fre, $expName, @componentNodes) = ($r->fre(), $r->name(), $r->node()->findnodes('component'));
  
  if (scalar(@componentNodes) > 0)
  {
    my %components;
    foreach my $componentNode (@componentNodes)
    {
      # ----------------------------------------- get and check the component name
      my $name = $r->nodeValue($componentNode, '@name');
      if ($name)
      {
	$fre->out(FREMsg::NOTE, "COMPONENTLOOP: ((($name)))");
	if (!exists($components{$name}))
	{
	  # ----------------------------------------------- get and check component data for compilation
	  my $paths = $strMergeWS->($r->nodeValue($componentNode, '@paths'));
	  if ($paths)
	  {
	    # -------------------------------------------------------------------- get and check include directories
	    my $includeDirs = $strMergeWS->($r->extractComponentValue('@includeDir', $name));
	    if ($includeDirs)
	    {
	      foreach my $includeDir (split(' ', $includeDirs))
	      {
        	if (! -d $includeDir)
        	{
        	  $fre->out(FREMsg::FATAL, "Component '$name' specifies non-existent include directory '$includeDir'");
  		  return 0;
		}
	      }
	    }
	    # --------------------------------------------- get and check library data; skip the component if the library defined  
	    my $libPath = $strRemoveWS->($r->extractComponentValue('library/@path', $name));
            my $libHeaderDir = $strRemoveWS->($r->extractComponentValue('library/@headerDir', $name));
	    if ($libPath)
	    {
	      if (-f $libPath)
	      {
		if ($libHeaderDir)
		{
		  if (-d $libHeaderDir)
		  {
        	    $fre->out(FREMsg::NOTE, "You have requested library '$libPath' for component '$name': we will skip the component compilation");
		  }
		  else
		  {
        	    $fre->out(FREMsg::FATAL, "Component '$name' specifies non-existent library header directory '$libHeaderDir'");
		    return 0;
		  }
		}
		else
		{
        	  $fre->out(FREMsg::FATAL, "Component '$name' specifies library '$libPath' but no header directory");
		  return 0;
		}
	      }
	      else
	      {
        	$fre->out(FREMsg::FATAL, "Component '$name' specifies non-existent library '$libPath'");
		return 0;
	      }	 
	    }
	    # ----------------------------------------------------------------------------------- save component data into the hash
            my %component = ();
	    $component{paths} = $paths;
	    $component{requires} = $strMergeWS->($r->nodeValue($componentNode, '@requires'));
	    $component{includeDirs} = $includeDirs;
            $component{libPath} = $libPath;
            $component{libHeaderDir} = $libHeaderDir;
	    $component{srcList} = $strMergeWS->($r->extractCompileValue('srcList', $name));
	    $component{pathNames} = $strMergeWS->($r->extractCompileValue('pathNames/@file', $name));
	    $component{cppDefs} = FREUtil::strStripPaired($strMergeWS->($r->extractCompileValue('cppDefs', $name)));
	    $component{makeOverrides} = $strMergeWS->($r->extractCompileValue('makeOverrides', $name));
	    $component{compileCsh} = $r->extractCompileValue('csh', $name);
	    $component{mkmfTemplate} = $strRemoveWS->($r->extractMkmfTemplate($name)) || $fre->mkmfTemplate();
            $component{lineNumber} = $componentNode->line_number();
	    $component{rank} = undef;
	    # ------------------------------------------------------------------------------------------- print what we got
	    $fre->out
	    (
	      FREMsg::NOTE,
	      "name            = $name",
	      "paths           = $component{paths}",
	      "requires        = $component{requires}",
	      "includeDir      = $component{includeDirs}",
	      "libPath         = $component{libPath}",
	      "libHeaderDir    = $component{libHeaderDir}",
	      "srcList         = $component{srcList}",
	      "pathNames       = $component{pathNames}",
	      "cppDefs         = $component{cppDefs}",
	      "makeOverrides   = $component{makeOverrides}",
	      "compileCsh      = $component{compileCsh}",
	      "mkmfTemplate    = $component{mkmfTemplate}"
	    );
	    # ------------------------------------------------------------ link the component to the components hash
	    $components{$name} = \%component;
	  }
	  else
	  {
	    $fre->out(FREMsg::FATAL, "Component '$name' doesn't specify the mandatory 'paths' attribute");
	    return 0; 
	  }
	}
	else
	{
	  $fre->out(FREMsg::FATAL, "Component '$name' is defined more than once - make sure each component has a distinct name");
	  return 0;
	}
      }
      else
      {
	$fre->out(FREMsg::FATAL, "Components with empty names aren't allowed");
	return 0;
      }
    }
    # ------------------------------------------------------------------ verify intercomponent references
    foreach my $name (keys %components)
    {
      my $ref = $components{$name};
      foreach my $required (split(' ', $ref->{requires}))
      {
        if (!exists($components{$required}))
	{
	  $fre->out(FREMsg::FATAL, "Component '$name' refers to a non-existent component '$required'");
	  return 0;
	}
      }
    }      
    # ------------------------------------------------------------------- compute components ranks      
    foreach my $name (keys %components)
    {
      my $ref = $components{$name};
      if (!defined($ref->{rank}))
      {
        if ($rankSet->(\%components, $ref, 0) < 0)
	{
	  $fre->out(FREMsg::FATAL, "Component '$name' refers to itself via a loop");
	  return 0;
	}
      }
    }
    # ------------------------------------------------------------------------ normal return
    return \%components;
  }
  else
  {
    $fre->out(FREMsg::FATAL, "The experiment '$expName' doesn't contain any components");
    return 0;
  }
     
}

sub extractRegressionLabels($$)
# ------ arguments: $object $regressionOption
{
  my ($r, $l) = @_;
  my ($fre, $expName, @expLabels) = ($r->fre(), $r->name(), $regressionLabels->($r));
  unless (my @expDuplicateLabels = FREUtil::listDuplicates(@expLabels))
  {
    my @optLabels = split(',', $l);
    my @optUnknownLabels = ();
    {
      foreach my $optLabel (@optLabels)
      {
	push @optUnknownLabels, $optLabel if $optLabel ne 'all' && $optLabel ne 'suite' && grep($_ eq $optLabel, @expLabels) == 0;
      }
    }
    if (scalar(@optUnknownLabels) == 0)
    {
      my @result = ();
      if (grep($_ eq 'all', @optLabels) > 0)
      {
        @result = @expLabels;
      }
      elsif (grep($_ eq 'suite', @optLabels) > 0)
      {
        foreach my $expLabel (@expLabels)
	{
	  push @result, $expLabel if grep($_ eq $expLabel, @optLabels) > 0 || grep($_ eq $expLabel, FREExperiment::REGRESSION_SUITE) > 0;
	}
      }
      else
      {
        foreach my $expLabel (@expLabels)
	{
	  push @result, $expLabel if grep($_ eq $expLabel, @optLabels) > 0;
	}
      } 
      return @result;
    }
    else
    {
      my $optUnknownLabels = join(', ', @optUnknownLabels);
      $fre->out(FREMsg::FATAL, "The experiment '$expName' doesn't contains regression tests '$optUnknownLabels'");
      return ();
    }
  }
  else
  {
    my $expDuplicateLabels = join(', ', @expDuplicateLabels);
    $fre->out(FREMsg::FATAL, "The experiment '$expName' contains non-unique regression tests '$expDuplicateLabels'");
    return ();
  }
}

sub extractRegressionRunInfo($$)
# ------ arguments: $object $label
# ------ called as object method
# ------ return a reference to the regression run info
{
  my ($r, $l) = @_;
  my ($fre, $expName) = ($r->fre(), $r->name());
  if (my $nmls = $r->extractNamelists())
  { 
    if (my $regNode = $regressionRunNode->($r, $l))
    {
      my @runNodes = $regNode->findnodes('run');
      if (scalar(@runNodes) > 0)
      {
	my ($ok, %runs) = (1, ());
	for (my $i = 0; $i < scalar(@runNodes); $i++)
	{
	  my $nps = $r->nodeValue($runNodes[$i], '@npes');
	  my $msl = $r->nodeValue($runNodes[$i], '@months');
	  my $dsl = $r->nodeValue($runNodes[$i], '@days');
	  my $hsl = $r->nodeValue($runNodes[$i], '@hours');
	  my $srt = $r->nodeValue($runNodes[$i], '@runTimePerJob');
	  my $patternRunTime = qr/^\d?\d:\d\d:\d\d$/;
	  if ($srt =~ m/$patternRunTime/)
	  {
	    if ($msl or $dsl or $hsl)
	    {
	      my @msa = split(' ', $msl);
	      my @dsa = split(' ', $dsl);
	      my @hsa = split(' ', $hsl);
	      my $spj = List::Util::max(scalar(@msa), scalar(@dsa), scalar(@hsa));
	      while (scalar(@msa) < $spj) {push(@msa, '0');}
	      while (scalar(@dsa) < $spj) {push(@dsa, '0');}
	      while (scalar(@hsa) < $spj) {push(@hsa, '0');}
	      my $nmlsOverridden = $overrideRegressionNamelists->($r, $nmls->copy(), $runNodes[$i]);
	      if (my $mpiInfo = $MPISizeParameters->($r, $nps, $nmlsOverridden))
	      {
        	my %run = ();
		$run{label} = $l;
		$run{number} = $i;
		$run{postfix} = $regressionPostfix->($r, $l, $i, $hsl, $spj, $msa[0], $dsa[0], $hsa[0], $mpiInfo);
		$run{mpiInfo} = $mpiInfo;
		$run{months} = join(' ', @msa);
		$run{days} = join(' ', @dsa);
		$run{hours} = join(' ', @hsa);
		$run{hoursDefined} = ($hsl ne "");
 		$run{runTimeMinutes} = FREUtil::makeminutes($srt);
		$run{namelists} = $nmlsOverridden;
		$runs{$i} = \%run;
              }
	      else
	      {
        	$fre->out(FREMsg::FATAL, "The experiment '$expName', the regression test '$l', run '$i' - model size parameters are invalid");
		$ok = 0; 
	      }
	    }
	    else
	    {
              $fre->out(FREMsg::FATAL, "The experiment '$expName', the regression test '$l', run '$i' - timing parameters must be defined");
	      $ok = 0; 
	    }
	  }
	  else
	  {
            $fre->out(FREMsg::FATAL, "The experiment '$expName', the regression test '$l', run '$i' - the running time '$srt' must be nonempty and have the HH:MM:SS format");
	    $ok = 0; 
	  }
	}
	return ($ok) ? \%runs : 0;
      }
      else
      {
	$fre->out(FREMsg::FATAL, "The experiment '$expName' - the regression test '$l' doesn't have any runs");
	return 0;
      }
    }
    else
    {
      $fre->out(FREMsg::FATAL, "The experiment '$expName' - the regression test '$l' doesn't exist or defined more than once");
      return 0;
    }
  }
  else
  {
    $fre->out(FREMsg::FATAL, "The experiment '$expName' - unable to extract namelists");
    return 0;
  }
}

sub extractProductionRunInfo($)
# ------ arguments: $object
# ------ called as object method
# ------ return a reference to the production run info
{
  my $r = shift;
  my ($fre, $expName) = ($r->fre(), $r->name());
  if (my $nmls = $r->extractNamelists())
  {
    if (my $prdNode = $productionRunNode->($r))
    {
      # get the namelist to get the proper number of npes
      my $nmlsOverridden = $overrideProductionNamelists->($r, $nmls->copy());
      # number of PEs as configured in runtime/production
      my $nps = $r->nodeValue($prdNode, '@npes');
      my $mpiInfo = $MPISizeParameters->($r, $nps, $nmlsOverridden);
      if ($mpiInfo)
      {
        # Use the number of PEs obtained from coupler_nml instead of runtime/production/@npes
        # as runtime/production/@npes does not take into account OpenMP threads in the pe count
        my $totNps = 0;
        for ( my $i = 0; $i < scalar($mpiInfo->{npesList}); $i++ )
        {
          $totNps += $mpiInfo->{npesList}[$i] * $mpiInfo->{ntdsList}[$i];
        }
        $nps = $totNps;
      }
      my $smt = $r->nodeValue($prdNode, '@simTime');
      my $smu = $r->nodeValue($prdNode, '@units');
      my $srt = $r->nodeValue($prdNode, '@runTime') || $fre->runTime($nps);
      my $gmt = $r->nodeValue($prdNode, 'segment/@simTime');
      my $gmu = $r->nodeValue($prdNode, 'segment/@units');
      my $grt = $r->nodeValue($prdNode, 'segment/@runTime');
      my $patternUnits = qr/^(?:years|year|months|month)$/;
      if (($smt > 0) and ($smu =~ m/$patternUnits/))
      {
        if (($gmt > 0) and ($gmu =~ m/$patternUnits/))
	{
	  my $patternYears = qr/^(?:years|year)$/;
	  $smt *= 12 if $smu =~ m/$patternYears/;
	  $gmt *= 12 if $gmu =~ m/$patternYears/;
	  if ($gmt <= $smt)
	  {
	    my $patternRunTime = qr/^\d?\d:\d\d:\d\d$/;
	    if ($srt =~ m/$patternRunTime/)
	    {
	      if ($grt =~ m/$patternRunTime/)
	      {
		my ($srtMinutes, $grtMinutes) = (FREUtil::makeminutes($srt), FREUtil::makeminutes($grt));
		if ($grtMinutes <= $srtMinutes)
		{
		  my $nmlsOverridden = $overrideProductionNamelists->($r, $nmls->copy());
		  if ($mpiInfo)
		  {
		    my %run = ();
		    $run{mpiInfo} = $mpiInfo;
		    $run{simTimeMonths} = $smt;
		    $run{simRunTimeMinutes} = $srtMinutes;
		    $run{segTimeMonths} = $gmt;
		    $run{segRunTimeMinutes} = $grtMinutes;
		    $run{namelists} = $nmlsOverridden;
		    return \%run;
                  }
		  else
		  {
		    $fre->out(FREMsg::FATAL, "The experiment '$expName' - model size parameters are invalid");
		    return 0; 
		  }
		}
		else
		{
		  $fre->out(FREMsg::FATAL, "The experiment '$expName' - the segment running time '$grtMinutes' must not exceed the maximum job running time allowed '$srtMinutes'");
		  return 0; 
		}
	      }
	      else
	      {
        	$fre->out(FREMsg::FATAL, "The experiment '$expName' - the segment running time '$grt' must be nonempty and have the HH:MM:SS format");
		return 0; 
	      }
	    }
	    else
	    {
              $fre->out(FREMsg::FATAL, "The experiment '$expName' - the simulation running time '$srt' must be nonempty and have the HH:MM:SS format");
	      return 0; 
	    }
	  }
	  else
	  {
	    $fre->out(FREMsg::FATAL, "The experiment '$expName' - the segment model time '$gmt' must not exceed the simulation model time '$smt'");
	    return 0; 
	  }
	}
	else
	{
          $fre->out(FREMsg::FATAL, "The experiment '$expName' - the segment model time '$gmt' must be nonempty and have one of (years|year|months|month) units defined");
	  return 0; 
	}
      }
      else
      {
        $fre->out(FREMsg::FATAL, "The experiment '$expName' - the simulation model time '$smt' must be nonempty and have one of (years|year|months|month) units defined");
	return 0; 
      }
    }
    else
    {
      $fre->out(FREMsg::FATAL, "The experiment '$expName' - production parameters aren't defined");
      return 0;
    }
  }
  else
  {
    $fre->out(FREMsg::FATAL, "The experiment '$expName' - unable to extract namelists");
    return 0;
  }
}

# //////////////////////////////////////////////////////////////////////////////
# //////////////////////////////////////////////////////////// Initialization //
# //////////////////////////////////////////////////////////////////////////////

return 1;
