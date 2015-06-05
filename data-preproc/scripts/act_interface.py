# Import packages
import os
import os.path as op
import warnings
import numpy as np
from nipype.interfaces.base import (TraitedSpec, File, InputMultiPath,
                                    OutputMultiPath, Undefined, traits,
                                    isdefined, OutputMultiPath,
                                    CommandLine, CommandLineInputSpec)
from nipype.utils.filemanip import split_filename
from nibabel import load

warn = warnings.warn
warnings.filterwarnings('always', category=UserWarning)


# Input spec class
class antsCorticalThicknessInputSpec(CommandLineInputSpec):
    dimension = traits.Int(argstr='-d %s', mandatory=True,
                     position=0, desc='image dimension')
    segmentation_iterations = traits.Int(argstr='-n %s',
                     postion=1, desc='N4 iterations during segmentation')
    segmentation_weight = traits.Float(argstr='-w %s',
                     position=2, desc='Atropos spatial prior probability' \
                          'weight for the segmentation')
    input_skull = File(exists=True, argstr='-a %s', mandatory=True,
                     position=3, desc='input file')
    template = File(exists=True, argstr='-e %s', mandatory=True,
                     position=4, desc='reference file')
    brain_prob_mask = File(exists=True, argstr='-m %s', mandatory=True,
                     position=5, desc='brain probability mask')
    brain_seg_priors = traits.Str(exists=True, argstr='-p %s', mandatory=True,
                     position=6, desc='brain segmentation priors')
    intensity_template = File(exists=True, argstr='-t %s', position=7,
                     desc='intensity template')
    extraction_registration_mask = File(exists=True, argstr='-f %s',
                     position=8, desc='extraction registration mask')
    out_prefix = traits.Str(exists=True, argstr='-o %s', mandatory=True,
                     position=9, desc='output prefix')
    keep_intermediate_files = traits.Int(True, argstr='-k %s', position=10,
                     desc='choose to delete intermediary files')


# Output spec class
class antsCorticalThicknessOutputSpec(TraitedSpec):
    brain_extraction_mask = File(exists=True,
                     desc='')
    brain_segmentation = File(exists=True,
                     desc='')
    brain_segmentation_N4 = File(exists=True,
                     desc='one for each anatomical input')
    brain_segmentation_posteriors_1 = File(exists=True,
                     desc='csf')
    brain_segmentation_posteriors_2 = File(exists=True,
                     desc='gm')
    brain_segmentation_posteriors_3 = File(exists=True,
                     desc='wm')
    brain_segmentation_posteriors_4 = File(exists=True,
                     desc='deep gm')
    brain_segmentation_posteriors_5 = File(exists=True,
                     desc='wm')
    brain_segmentation_posteriors_6 = File(exists=True,
                     desc='deep gm')
    brain_segmentation_posteriors_N = File(exists=True,
                     desc='one for each prior')
#    extracted_anatomical_brain = File(exists=True,
#                     desc='skull-stripped anatomical brain')
    ants_registration_affine = File(exists=True,
                     desc='one of the antsRegistration warps')
    ants_registration_warp = File(exists=True,
                     desc='one of the antsRegistration warps (non-linear)')
    cortical_thickness = File(exists=True,
                     desc='')
    cortical_thickness_normalized = File(exists=True,
                     desc='')


# Cortical thickness node
class antsCorticalThickness(CommandLine):

    _cmd = 'antsCorticalThickness.sh'
    input_spec = antsCorticalThicknessInputSpec
    output_spec = antsCorticalThicknessOutputSpec

    def _list_outputs(self):
        outputs = self.output_spec().get()
        outputs['brain_extraction_mask'] = os.path.abspath('OUTPUT_BrainExtractionMask.nii.gz')
        outputs['brain_segmentation'] = os.path.abspath('OUTPUT_BrainSegmentation.nii.gz')
        outputs['brain_segmentation_N4'] = os.path.abspath('OUTPUT_BrainSegmentation0N4.nii.gz')
        outputs['brain_segmentation_posteriors_1'] = os.path.abspath('OUTPUT_BrainSegmentationPosteriors1.nii.gz')
        outputs['brain_segmentation_posteriors_2'] = os.path.abspath('OUTPUT_BrainSegmentationPosteriors2.nii.gz')
        outputs['brain_segmentation_posteriors_3'] = os.path.abspath('OUTPUT_BrainSegmentationPosteriors3.nii.gz')
        outputs['brain_segmentation_posteriors_4'] = os.path.abspath('OUTPUT_BrainSegmentationPosteriors4.nii.gz')
        outputs['brain_segmentation_posteriors_5'] = os.path.abspath('OUTPUT_BrainSegmentationPosteriors5.nii.gz')
        outputs['brain_segmentation_posteriors_6'] = os.path.abspath('OUTPUT_BrainSegmentationPosteriors6.nii.gz')
#        outputs['extracted_anatomical_brain'] = os.path.abspath('OUTPUT_ExtractedBrain0N4.nii.gz')
        outputs['ants_registration_affine'] = os.path.abspath('OUTPUT_SubjectToTemplate0GenericAffine.mat')
        outputs['ants_registration_warp'] = os.path.abspath('OUTPUT_SubjectToTemplate1Warp.nii.gz')
        outputs['cortical_thickness'] = os.path.abspath('OUTPUT_CorticalThickness.nii.gz')
        outputs['cortical_thickness_normalized'] = os.path.abspath('OUTPUT_CorticalThicknessNormalizedToTemplate.nii.gz')
        return outputs


