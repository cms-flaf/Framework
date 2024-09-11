import ROOT as rt

# CMS style text configuration
cmsText = "CMS"
cmsTextFont = 61  # Font for "CMS"
cmsTextSize = 0.3
cmsTextOffset = 0.4
relPosX = 0.06  # CMS text position (X)
relPosY = 0.1  # CMS text position (Y)


writeExtraText = True
extraText = "Preliminary"
extraTextSize = cmsTextSize * 0.7
extraTextFont = 58  # Font for the "Preliminary"
relExtraDY = 0.4  # Distance between CMS text and extra text
relExtraDX = 0.01  # Distance between CMS text and extra text

extraOverCmsTextSize = 0.1  # Extra text size relative to CMS text size

lumiTextSize = 0.3
lumiTextOffset = 0.1
lumiTextFont = 42  # Font for "CMS"
lumi_13TeV = "138.8 fb^{-1} (13 TeV)"
lumi_sqrtS = ""
period_dict = {
    "Run2_2018" : "59.83 fb^{-1} (13 TeV)",
    "Run2_2017" : "41.48 fb^{-1} (13 TeV)",
    "Run2_2016" : "16.8 fb^{-1} (13 TeV)",
    "Run2_2016_H" : "19.5 fb^{-1} (13 TeV)",
    "all": "138.8 fb^{-1} (13 TeV)",

}

def CMS_lumi(pad, iPeriod, iPosX):
    """Function to add CMS text and luminosity to the plot."""
    outOfFrame = False
    if(iPosX // 10 == 0): outOfFrame = True

    alignY_ = 3
    alignX_ = 2
    if(iPosX // 10 == 0): alignX_ = 1
    if(iPosX == 0): alignY_ = 1
    if(iPosX // 10 == 1): alignX_ = 1
    if(iPosX // 10 == 2): alignX_ = 2
    if(iPosX // 10 == 3): alignX_ = 3
    align_ = 10 * alignX_ + alignY_

    H = pad.GetWh()
    W = pad.GetWw()
    leftMargin = pad.GetLeftMargin()
    topMargin = pad.GetTopMargin()
    rightMargin = pad.GetRightMargin()
    bottomMargin = pad.GetBottomMargin()

    pad.cd()
    lumiText = ""
    lumiText += period_dict[iPeriod]

    latex = rt.TLatex()
    latex.SetNDC()
    latex.SetTextAngle(0)
    latex.SetTextColor(rt.kBlack)
    latex.SetTextFont(lumiTextFont)
    latex.SetTextAlign(31)  # Align right
    latex.SetTextSize(lumiTextSize * topMargin)

    # Draw luminosity text on the right
    latex.DrawLatex(1 - rightMargin, 1 - topMargin + lumiTextOffset * topMargin, lumiText)

    pad.cd()

    posX_ = leftMargin + relPosX*leftMargin # CMS text on the left
    posY_ = 1 - relPosY #+ cmsTextOffset
    #posY_ = 1 - 0.4*topMargin

    # Draw the CMS text on the left
    latex.SetTextFont(cmsTextFont)
    latex.SetTextSize(cmsTextSize * topMargin)
    latex.SetTextAlign(13)  # Align left and top
    latex.DrawLatex(posX_, posY_, cmsText)

    # Draw extra text below CMS (e.g., "Preliminary")
    if(writeExtraText):
        latex.SetTextFont(extraTextFont)
        latex.SetTextAlign(13)
        latex.SetTextSize(extraTextSize*topMargin)
        latex.DrawLatex(posX_+relExtraDX*leftMargin, posY_, extraText)

    pad.Update()